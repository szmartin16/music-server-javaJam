package main.java;

import java.io.*;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class MatchmakingServer {
    private static final int PORT = 8080;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final ConcurrentHashMap<String, SessionRoom> rooms = new ConcurrentHashMap<>();

    private static void broadcastClientCount(SessionRoom room) {
        if (room.hostSocket != null && !room.hostSocket.isClosed()) {
            try {
                PrintWriter hostWriter = new PrintWriter(room.hostSocket.getOutputStream(), true);
                hostWriter.println("STATUS|CLIENT_COUNT|" + room.clientSockets.size());
            } catch (IOException ignored) {}
        }
    }

    public static void main(String[] args) {
        log("INFO", "[RELAY SERVER] Initializing matchmaking server...");
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            log("INFO", "[RELAY SERVER] Successfully started and listening on port " + PORT);

            while (true) {
                Socket socket = serverSocket.accept();
                log("INFO", "[SERVER] Incoming connection accepted from: " + socket.getRemoteSocketAddress());
                new Thread(new ConnectionHandler(socket)).start();
            }
        } catch (IOException e) {
            log("ERROR", "[RELAY SERVER ERROR] Server socket encountered a fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void log(String level, String message) {
        String timestamp = LocalDateTime.now().format(DATE_FORMATTER);
        System.out.println(String.format("[%s] [%s] %s", timestamp, level, message));
    }

    private static class SessionRoom {
        Socket hostSocket;
        List<Socket> clientSockets = new CopyOnWriteArrayList<>();
    }

    private static class ConnectionHandler implements Runnable {
        private final Socket socket;

        public ConnectionHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String remoteAddress = socket.getRemoteSocketAddress().toString();
            String sessionId = null;
            String role = null;
            SessionRoom room = null;

            try {
                BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"));
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                String authLine = reader.readLine();
                if (authLine == null || !authLine.startsWith("AUTH|")) {
                    log("WARN", "[AUTH] Invalid or empty handshake received from " + remoteAddress + ". Closing connection.");
                    return;
                }

                String[] parts = authLine.split("\\|");
                if (parts.length < 3) {
                    log("WARN", "[AUTH] Malformed handshake format from " + remoteAddress + ": " + authLine);
                    return;
                }

                role = parts[1];
                sessionId = parts[2];

                if (!"HOST".equals(role) && !"CLIENT".equals(role)) {
                    log("WARN", "[AUTH] Unknown role '" + role + "' specified by " + remoteAddress);
                    return;
                }

                rooms.putIfAbsent(sessionId, new SessionRoom());
                room = rooms.get(sessionId);

                if ("HOST".equals(role)) {
                    room.hostSocket = socket;
                    log("INFO", "[ROOM " + sessionId + "] HOST successfully connected from " + remoteAddress);
                } else {
                    room.clientSockets.add(socket);
                    log("INFO", "[ROOM " + sessionId + "] CLIENT connected from " + remoteAddress + ". Total active clients in room: " + room.clientSockets.size());
                    broadcastClientCount(room);
                }

                String line;
                while ((line = reader.readLine()) != null) {
                    if ("HOST".equals(role)) {
                        for (Socket client : room.clientSockets) {
                            try {
                                new PrintWriter(client.getOutputStream(), true).println(line);
                            } catch (IOException ignored) {}
                        }
                    } else {
                        if (room.hostSocket != null && !room.hostSocket.isClosed()) {
                            try {
                                new PrintWriter(room.hostSocket.getOutputStream(), true).println(line);
                            } catch (IOException ignored) {}
                        }
                    }
                }

            } catch (SocketException e) {
                log("WARN", "[CONNECTION] Socket reset or abruptly closed for " + remoteAddress + ": " + e.getMessage());
            } catch (IOException e) {
                log("ERROR", "[CONNECTION ERROR] I/O failure for " + remoteAddress + ": " + e.getMessage());
            } finally {
                // Centralized cleanup runs whether connection closes normally or via exception
                if (sessionId != null && room != null) {
                    if ("HOST".equals(role)) {
                        log("INFO", "[ROOM " + sessionId + "] HOST disconnected (" + remoteAddress + "). Closing room and disconnecting all clients.");
                        rooms.remove(sessionId);
                        for (Socket client : room.clientSockets) {
                            try { client.close(); } catch (IOException ignored) {}
                        }
                    } else if ("CLIENT".equals(role)) {
                        room.clientSockets.remove(socket);
                        log("INFO", "[ROOM " + sessionId + "] CLIENT disconnected (" + remoteAddress + "). Remaining active clients: " + room.clientSockets.size());
                        broadcastClientCount(room);
                    }
                }

                try {
                    socket.close();
                } catch (IOException ignored) {}
            }
        }
    }
}