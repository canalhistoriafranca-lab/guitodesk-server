/**
 * GuitoDesk — Servidor de Sinalização
 *
 * Protocolo: WebSocket puro com framing Socket.IO simplificado
 *   - Cliente envia "40" → registra no namespace
 *   - Cliente envia "42[\"evento\", {...}]" → evento com dados
 *   - Servidor envia "42[\"evento\", {...}]" → evento com dados
 *   - Servidor envia "2" (ping) a cada 25s, cliente responde "3" (pong)
 *
 * Não usa socket.io no servidor — usa a lib 'ws' diretamente para ser
 * compatível com o cliente Qt que implementa o protocolo manualmente.
 */

const { WebSocketServer } = require('ws');
const http  = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;

// ── Estado do servidor ────────────────────────────────────────────────────────
// deviceId → { ws, deviceId, password, alias, socketId }
const devices  = new Map();
// socketId → device info
const sockets  = new Map();
// sessionId → { hostSocketId, viewerSocketId }
const sessions = new Map();

// ── HTTP server (health check + CORS) ─────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
    res.setHeader('Access-Control-Allow-Origin', '*');
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({
            status: 'ok',
            devices: devices.size,
            sessions: sessions.size,
            uptime: Math.floor(process.uptime()) + 's'
        }));
    } else {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.end('GuitoDesk Signaling Server v1.0\n');
    }
});

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server: httpServer, path: '/socket.io/' });

wss.on('connection', (ws, req) => {
    const socketId = crypto.randomUUID();
    ws._socketId = socketId;
    ws._alive    = true;
    sockets.set(socketId, { ws, socketId, deviceId: null });

    console.log(`[+] Conectado: ${socketId} (${req.socket.remoteAddress})`);

    // Socket.IO handshake: envia "0{...}" e depois aguarda "40"
    const handshake = JSON.stringify({
        sid: socketId,
        upgrades: [],
        pingInterval: 25000,
        pingTimeout: 5000,
        maxPayload: 1000000
    });
    ws.send('0' + handshake);

    ws.on('message', (raw) => {
        const msg = raw.toString();

        if (msg === '2')  { ws._alive = true; ws.send('3'); return; } // pong
        if (msg === '3')  { ws._alive = true; return; }               // pong do cliente
        if (msg === '40') { ws.send('40'); return; }                  // namespace ACK

        if (!msg.startsWith('42')) return;

        // Remove "42" e possível id de callback, isola o array JSON
        const bracketIdx = msg.indexOf('[');
        if (bracketIdx < 0) return;
        let arr;
        try { arr = JSON.parse(msg.slice(bracketIdx)); }
        catch { return; }

        const [event, data] = arr;
        handleEvent(ws, socketId, event, data || {});
    });

    ws.on('pong', () => { ws._alive = true; });

    ws.on('close', () => {
        console.log(`[-] Desconectado: ${socketId}`);
        handleDisconnect(socketId);
    });

    ws.on('error', (err) => {
        console.error(`[!] Erro socket ${socketId}:`, err.message);
    });
});

// ── Ping keepalive ────────────────────────────────────────────────────────────
setInterval(() => {
    wss.clients.forEach(ws => {
        if (!ws._alive) { ws.terminate(); return; }
        ws._alive = false;
        ws.send('2');   // Socket.IO ping
    });
}, 25000);

// ── Enviar evento para um socket ──────────────────────────────────────────────
function emit(ws, event, data) {
    if (!ws || ws.readyState !== 1) return;
    ws.send('42' + JSON.stringify([event, data]));
}

// ── Handlers de evento ────────────────────────────────────────────────────────
function handleEvent(ws, socketId, event, data) {
    console.log(`[>] ${socketId.slice(0,8)} → ${event}`, JSON.stringify(data).slice(0, 120));

    switch (event) {

        case 'register': {
            // Registra o dispositivo com ID persistente
            let { deviceId, password, alias } = data;

            // Gera novo ID se não tiver ou já estiver em uso por outro socket
            if (!deviceId || (devices.has(deviceId) && devices.get(deviceId).socketId !== socketId)) {
                deviceId = generateDeviceId();
            }
            if (!password) {
                password = generatePassword();
            }

            // Remove registro anterior deste socket
            const prev = sockets.get(socketId);
            if (prev?.deviceId) devices.delete(prev.deviceId);

            const info = { ws, deviceId, password, alias: alias || 'PC', socketId };
            devices.set(deviceId, info);
            sockets.set(socketId, { ...sockets.get(socketId), ...info });

            emit(ws, 'registered', { deviceId, password });
            console.log(`[✓] Registrado: ${deviceId} (${alias})`);
            break;
        }

        case 'request-connection': {
            const { targetId, password, alias } = data;
            const target = devices.get(targetId);

            if (!target) {
                emit(ws, 'connect-result', { success: false, error: 'Dispositivo não encontrado.' });
                return;
            }

            const sessionId = crypto.randomUUID();
            sessions.set(sessionId, { hostSocketId: target.socketId, viewerSocketId: socketId, sessionId });

            // Modo não supervisionado: senha fornecida e correta → aceita direto
            if (password && target.password && password === target.password) {
                emit(viewerInfo?.ws ?? ws, 'connection-accepted', { sessionId });
                emit(target.ws, 'connection-request-unattended', { sessionId, viewerSocketId: socketId });
                console.log(`[✓] Conexão não supervisionada: ${sessionId}`);
                return;
            }

            // Senha errada fornecida → rejeita
            if (password && target.password && password !== target.password) {
                sessions.delete(sessionId);
                emit(ws, 'connect-result', { success: false, error: 'Senha incorreta.' });
                return;
            }

            // Modo supervisionado: sem senha → mostra popup de aceite no host
            emit(target.ws, 'connection-request', {
                sessionId,
                viewerSocketId: socketId,
                viewerAlias: alias || 'Operador'
            });
            console.log(`[>] Solicitação supervisionada: ${sessionId}`);
            break;
        }

        case 'connection-response': {
            const { sessionId, accepted, viewerSocketId } = data;
            const session = sessions.get(sessionId);
            if (!session) return;

            const viewerInfo = sockets.get(viewerSocketId || session.viewerSocketId);
            if (!viewerInfo) return;

            if (accepted) {
                emit(viewerInfo.ws, 'connection-accepted', { sessionId });
                console.log(`[✓] Sessão aceita: ${sessionId}`);
            } else {
                emit(viewerInfo.ws, 'connection-rejected', {
                    sessionId,
                    reason: 'O host recusou a conexão.'
                });
                sessions.delete(sessionId);
                console.log(`[✗] Sessão recusada: ${sessionId}`);
            }
            break;
        }

        case 'webrtc-signal': {
            // Relay do sinal para o outro lado da sessão (frames de vídeo, input, etc.)
            const { sessionId, signal } = data;
            const session = sessions.get(sessionId);
            if (!session) return;

            // Descobre para quem mandar (o lado oposto de quem enviou)
            const destId = session.hostSocketId === socketId
                ? session.viewerSocketId
                : session.hostSocketId;

            const dest = sockets.get(destId);
            if (dest?.ws) {
                emit(dest.ws, 'webrtc-signal', { sessionId, signal });
            }
            break;
        }

        case 'chat-message': {
            const { sessionId, message, sender } = data;
            const session = sessions.get(sessionId);
            if (!session) return;

            const destId = session.hostSocketId === socketId
                ? session.viewerSocketId
                : session.hostSocketId;

            const dest = sockets.get(destId);
            if (dest?.ws) {
                emit(dest.ws, 'chat-message', { sessionId, message, sender });
            }
            break;
        }

        case 'end-session': {
            const { sessionId } = data;
            const session = sessions.get(sessionId);
            if (!session) return;

            // Notifica ambos os lados
            [session.hostSocketId, session.viewerSocketId].forEach(id => {
                const info = sockets.get(id);
                if (info?.ws && info.socketId !== socketId) {
                    emit(info.ws, 'session-ended', { sessionId, reason: 'Sessão encerrada.' });
                }
            });

            sessions.delete(sessionId);
            console.log(`[-] Sessão encerrada: ${sessionId}`);
            break;
        }

        default:
            console.log(`[?] Evento desconhecido: ${event}`);
    }
}

function handleDisconnect(socketId) {
    const info = sockets.get(socketId);
    if (!info) return;

    // Remove o dispositivo
    if (info.deviceId) {
        devices.delete(info.deviceId);
        console.log(`[-] Dispositivo removido: ${info.deviceId}`);
    }

    // Encerra sessões ativas deste socket
    for (const [sessionId, session] of sessions.entries()) {
        if (session.hostSocketId === socketId || session.viewerSocketId === socketId) {
            const otherId = session.hostSocketId === socketId
                ? session.viewerSocketId
                : session.hostSocketId;

            const other = sockets.get(otherId);
            if (other?.ws) {
                emit(other.ws, 'session-ended', {
                    sessionId,
                    reason: 'O outro lado se desconectou.'
                });
            }
            sessions.delete(sessionId);
        }
    }

    sockets.delete(socketId);
}

// ── Geradores ─────────────────────────────────────────────────────────────────
function generateDeviceId() {
    let id;
    do { id = String(Math.floor(100000000 + Math.random() * 900000000)); }
    while (devices.has(id));
    return id;
}

function generatePassword() {
    const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
    return Array.from({ length: 8 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

// ── Inicia ────────────────────────────────────────────────────────────────────
httpServer.listen(PORT, () => {
    console.log(`\n╔══════════════════════════════════╗`);
    console.log(`║   GuitoDesk Signaling Server     ║`);
    console.log(`╠══════════════════════════════════╣`);
    console.log(`║  Porta : ${PORT.toString().padEnd(24)}║`);
    console.log(`║  Health: http://localhost:${PORT}/health ║`);
    console.log(`╚══════════════════════════════════╝\n`);
});
