import {randomUUID, randomBytes, createHash} from "crypto"
import DateTools from "@hackthedev/datetools"
import AuthTools from "@hackthedev/dsync-auth";
import Logger from "@hackthedev/terminal-logger"

function generateRandomString() {
    return (Math.random().toString(36).slice(2)) + (Math.random().toString(36).slice(2))
}

function getStringSize(value) {
    return new TextEncoder().encode(String(value)).byteLength / 1024;
}

export default class dSyncInbox {
    constructor({
                    io = null,
                    app = null,
                    express = null,
                    dSyncSign = null,
                    dSyncSql = null,
                    dSyncAuth = null,
                    isValidated = null,
                    getIdentifier = null,
                    beforeReturn = null,
                } = {}) {

        this.io = io;
        this.signer = dSyncSign;
        this.db = dSyncSql;
        this.express = express
        this.auther = dSyncAuth

        this.isValidated = typeof isValidated === "function" ? isValidated : null;
        this.getIdentifier = typeof getIdentifier === "function" ? getIdentifier : null;
        this.beforeReturn = typeof beforeReturn === "function" ? beforeReturn : null;

        if (!isValidated) throw new Error("No isValidated function provided");
        if (!getIdentifier) throw new Error("No getIdentifier function provided");

        if (!io) {
            console.error("socket io is required!")
            process.exit(0)
        }

        if (!app) {
            console.error("Express app is required!")
            process.exit(0)
        }

        if (!express) {
            console.error("Express is required!")
            process.exit(0)
        }

        if (!dSyncSign) {
            console.error("dSyncSign is required!")
            process.exit(0)
        }

        if (!dSyncAuth) {
            console.error("dSyncAuth is required!")
            process.exit(0)
        }

        if (!dSyncSql) {
            console.error("dSyncSql is required!")
            process.exit(0)
        }

        app.use((req, res, next) => {
            const origin = req.headers.origin;

            res.header("Access-Control-Allow-Origin", "*");
            res.header("Vary", "Origin");
            res.header("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS");
            res.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
            res.header("Access-Control-Max-Age", "86400");
            res.set("Cache-Control", "no-store");

            if (req.method === "OPTIONS") {
                return res.sendStatus(204);
            }

            next();
        });

        app.get(`/messenger/profile/:identifier`, this.express.json(), async (req, res) => {
            const {identifier} = req?.params;

            if(!identifier) return res.status(403).json({error: "Missing identifier"})

            let gidInbox = await this.getGidTable(identifier);
            if(!gidInbox?.rowId) return res.status(404).json({error: "User not found"})

            let targetReturnObj = await this.getTargetReturnObject(gidInbox);
            return res.status(200).json({
                error: null,
                target: {
                    ...targetReturnObj
                }
            })
        });

        app.post(`/messenger/fetch{/:timestamp}{/:inboxId}{/:customId}`, this.express.json(), async (req, res) => {
            const {inboxId, timestamp, customId} = req?.params;
            if (!await this.isValidated(req, res)) return res.status(403).json({error: "Forbidden"})

            let identifier = await this.getIdentifier(req, res);
            if (!identifier) return res.status(403).json({error: "Missing identifier"})

            let messages = null;
            if (!inboxId && !timestamp) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE targetId = ? ORDER BY createdAt DESC LIMIT 50",
                    [identifier]
                );
            }
            if (!inboxId && timestamp) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE identifier = ? created < ? ORDER BY createdAt DESC LIMIT 50",
                    [identifier, timestamp]
                );
            }
            if (inboxId && !customId) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE id = ? AND identifier = ? LIMIT 50",
                    [inboxId, identifier]
                );
            }
            if (!inboxId && customId) {
                messages = await this.db.queryDatabase(
                    "SELECT * FROM inbox WHERE customId = ? AND identifier = ? LIMIT 50",
                    [inboxId, identifier]
                );
            }

            if (messages?.length > 0) {
                for (let message of messages) {
                    if (message?.data?.startsWith("{") && typeof message?.data === "string") message.data = JSON.parse(message.data);
                }
            }

            if (this.beforeReturn) await this.beforeReturn(req, res, messages)

            try {
                res.status(200).json({inbox: messages});
            } catch (err) {
                res.status(400).json({error: "Failed to solve challenge"})
            }
        })

        this.io.on("connection", async (socket) => {
            // socket ip
            var ip = this.getSocketIp(socket);

            socket.on("/messenger/hello", async (user, response) => {
                if (!await this.validateSocketAuth(user?.sessionId, user?.publicKey, response)) return;

                let userGid = this.signer.generateGid(user?.publicKey);
                if (!userGid) return response({error: "Failed to generate gid"})

                if(getStringSize(user?.profile?.signature) > 10) return response({ error: "Signature too long!" })
                if(getStringSize(user?.profile?.banner) > 10) return response({ error: "Banner URL too long!" })
                if(getStringSize(user?.profile?.icon) > 10) return response({ error: "Icon URL too long!" })
                if(getStringSize(user?.profile?.name) > 5) return response({ error: "Name too long!" })

                if(getStringSize(user?.vanity) > 5) return response({ error: "vanity too long!" })
                if(getStringSize(user?.publicKey) > 10) return response({ error: "public key too long!" })

                // set table etc
                let updateError = false;
                if (!user?.home_server) return response({error: "Requesting Home Server! (home_server)"})
                let gidTableResult = await this.updateGidTable({
                    gid: userGid,
                    home_server: user?.home_server,
                    publicKey: user?.publicKey,
                    vanity: user?.vanity ?? null,
                })

                // optionally update profile stuff
                if(user?.profile){
                    let existingProfile = await this.getGidTable(userGid);
                    if(!existingProfile?.rowId) return response({error: "Gid Table ID not found for profile!?"})

                    // try updating profile
                    let profileTableResult = await this.updateProfileTable({
                        gid_table_ref: existingProfile?.rowId,
                        icon: user?.profile?.icon ?? null,
                        banner: user?.profile?.banner ?? null,
                        name: user?.profile?.name ?? null,
                        signature: user?.profile?.signature ?? null,
                    })

                    // show error otherwise
                    if (![0, 1, 2].includes(profileTableResult?.affectedRows)) {
                        Logger.warn("Messenger profile Table insert/update warning!")
                        console.log(gidTableResult)
                        updateError = gidTableResult?.error ?? null;
                    }
                }

                if (![0, 1, 2].includes(gidTableResult?.affectedRows)) {
                    Logger.warn("Messenger GID Table insert/update warning!")
                    console.log(gidTableResult)
                    updateError += gidTableResult?.error ?? null;
                }

                // join own room to emit messages to
                if (!await this.targetIsOnline(userGid)) {
                    socket.join(userGid);
                }

                response({error: updateError ?? null})
            })

            socket.on("/messenger/send", async (user, response) => {
                if (!await this.validateSocketAuth(user?.sessionId, user?.message?.author?.publicKey, response)) return;

                if (!user?.message || typeof user?.message !== "object") return response({error: "No message object provided"})
                if (!user?.message?.type) return response({error: "No message type provided"})

                if (!user?.message?.author?.publicKey) return response({error: "No public key provided"})
                if (!user?.message?.targetIdentifier) return response({error: "No target identifier provided"})

                if(getStringSize(user?.message) > 15) return response({ error: "Message too long!" })
                if(getStringSize(user?.message?.author?.publicKey) > 10) return response({ error: "public key too long!" })

                let targetData = await this.getGidTable(user?.message?.targetIdentifier);
                let targetProfile = await this.getProfile(targetData?.rowId);
                let targetPublicKey = targetData?.publicKey;

                if (!targetPublicKey) return response({error: "No public key found of target"})

                let userGid = this.signer.generateGid(user?.message?.author?.publicKey);
                let targetGid = this.signer.generateGid(targetPublicKey);

                // if target is online send it directly to them
                if (!user?.message?.test) this.emitToGid(targetGid, "/messenger/receive", user.message);
                if (!user?.message?.test) this.emitToGid(userGid, "/messenger/receive", user.message);

                // temporarily save the message
                if (!user?.message?.test) {
                    this.setInboxEntry({
                        customId: user?.message?.timestamp ?? null,
                        targetId: targetGid,
                        type: "messenger_user-message",
                        data: user.message,
                        expiresAt: DateTools.getDateFromOffset("3 days").getTime(),
                        customId: user.message.timestamp
                    })
                }

                let targetReturnObj = await this.getTargetReturnObject(targetData);
                response({
                    error: null,
                    target: {
                        ...targetReturnObj
                    }
                })
            })

            socket.on("/messenger/fetch", async (user, response) => {
                if (!await this.validateSocketAuth(user?.sessionId, user?.publicKey, response)) return;

                let userGid = this.signer.generateGid(user?.publicKey);
                if (!userGid) return response({error: "Failed to generate gid"})

                let timestamp = Number(user?.timestamp ?? 0);
                let limit = Number(user?.limit ?? 50);

                if (limit > 100) limit = 100;
                if (limit < 1) limit = 50;

                let messages = null;

                if (timestamp > 0) {
                    messages = await this.db.queryDatabase(
                        `SELECT *
                         FROM inbox
                         WHERE targetId = ?
                           AND type = ?
                           AND createdAt > ?
                         ORDER BY createdAt ASC
                             LIMIT ?`,
                        [userGid, "messenger_user-message", timestamp, limit]
                    );
                } else {
                    messages = await this.db.queryDatabase(
                        `SELECT *
                         FROM inbox
                         WHERE targetId = ?
                           AND type = ?
                         ORDER BY createdAt ASC
                             LIMIT ?`,
                        [userGid, "messenger_user-message", limit]
                    );
                }

                for (let message of messages ?? []) {
                    if (typeof message?.data === "string" && message.data.startsWith("{")) {
                        message.data = JSON.parse(message.data);
                    }
                }

                response({
                    error: null,
                    inbox: messages ?? [],
                    latestTimestamp: messages?.length
                        ? Math.max(...messages.map(m => Number(m.createdAt ?? 0)))
                        : timestamp
                });
            });
        })
    }

    async getTargetReturnObject(gidObj){
        if(!gidObj?.rowId) throw new Error("No rowId provided");

        let profileObj = await this.getProfile(gidObj.rowId);

        return {
            gid: gidObj?.gid ?? null,
            vanity: gidObj.vanity ?? null,
            publicKey: gidObj.publicKey ?? null,
            home_server: gidObj.home_server ?? null,
            updatedAt: gidObj.updatedAt ?? null,
            createdAt: gidObj.createdAt ?? null,
            isOnline: await this.targetIsOnline(gidObj.gid) ?? null,

            profile: {
                name: profileObj?.name ?? null,
                banner: profileObj?.banner ?? null,
                signature: profileObj?.signature ?? null,
                icon: profileObj?.icon ?? null,
            }
        }
    }

    async targetIsOnline(targetGid){
        return this.io.sockets.adapter.rooms.has(targetGid);
    }

    async emitToGid(targetGid, event, data) {
        this.io.to(`${targetGid}`).emit(event, {...data})
    }

    async validateSocketAuth(sessionId, publicKey, response) {
        if (!sessionId || !publicKey) {
            response?.({error: "Authentication failed"})
            return false
        }

        let sessionResult = AuthTools.verifySession(this.auther.authSessions, sessionId, publicKey);
        let result = sessionResult?.valid ?? false;

        if (!result && response) {
            response({error: "Authentication failed - Result invalid"})
            return false;
        } else if (result === true) {
            return true;
        }

        response({error: "Authentication failed"})
        return false;
    }

    getSocketIp(socket) {
        return socket?.handshake?.headers["x-forwarded-for"]?.split(",")[0].trim()
            || socket?.handshake?.headers["x-real-ip"]
            || socket?.handshake?.address;
    }

    async setInboxEntry({
                            targetId = null,
                            type = null,
                            data = null,
                            isRead = null,
                            customId = null,
                            expiresAt = null,
                        } = {}) {
        if (!type?.trim()?.length === 0) throw new Error("type is required!")
        if (!targetId?.trim()?.length === 0) throw new Error("targetId is required!")
        if (!data || typeof data !== "object") throw new Error("data is required and must be a json object!")
        if (!customId) throw new Error("customId is required for identification!")

        return await this.db.queryDatabase(
            `INSERT INTO inbox (id, targetId, type, data, isRead, customId, expiresAt)
             VALUES (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY
            UPDATE
                isRead = IF(isRead IS NULL, VALUES (isRead), isRead)`,
            [randomUUID(), targetId, type, JSON.stringify(data, null, 4), isRead, customId, expiresAt]
        );
    }

    async updateGidTable({
                             gid = null,
                             publicKey = null,
                             home_server = null,
                             vanity = null,
                         } = {}) {
        if (!gid?.trim()) return {error: "GID missing!"}
        if (!publicKey?.trim()) return {error: "Public Key missing!"}
        if (!home_server?.trim()) return {error: "Home Server missing!"}

        let calculatedGid = await this.signer.generateGid(publicKey);
        if (calculatedGid !== gid) return {error: "GID and Public Key Mismatch!"}

        // check for duplicate
        if (vanity?.trim()) {
            let vanityRows = await this.db.queryDatabase(
                `SELECT gid FROM inbox_gid_table WHERE vanity = ? AND gid != ? LIMIT 1`,
                [vanity, gid]
            );

            if (vanityRows?.length > 0) {
                return {error: "Vanity already taken!"}
            }
        }

        return await this.db.queryDatabase(
            `INSERT INTO inbox_gid_table (gid, publicKey, home_server, updatedAt, vanity)
             VALUES (?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                                      updatedAt = (UNIX_TIMESTAMP() * 1000),
                                      home_server = VALUES(home_server),
                                      vanity = COALESCE(VALUES(vanity), vanity)`,
            [gid, publicKey, home_server, null, vanity]
        );
    }

    async getGidTable(identifier) {
        if (!identifier) return {error: "Missing identifier!"}

        let row = await this.db.queryDatabase(
            `SELECT * FROM inbox_gid_table WHERE gid = ? OR publicKey = ? OR vanity = ? OR rowId = ? LIMIT 1`,
            [identifier, identifier, identifier, identifier]
        );

        return row[0] ? row[0] : null;
    }

    async getProfile(identifier) {
        if (!identifier) return {error: "Missing identifier!"}

        let gidEntryRow = await this.getGidTable(identifier);
        if(!gidEntryRow || !gidEntryRow?.rowId) return {error: "Missing gid entry!"}

        let profile = await this.db.queryDatabase(
            `SELECT * FROM inbox_profiles WHERE gid_table_ref = ? LIMIT 1`,
            [gidEntryRow?.rowId]
        );

        return profile[0] ? profile[0] : null;
    }

    async updateProfileTable({
                             gid_table_ref = null,
                             name = null,
                             icon = null,
                             banner = null,
                             signature = null,
                         } = {}) {
        if (!gid_table_ref) return {error: "gid_table_ref missing!"}

        let gidEntryRow = await this.getGidTable(gid_table_ref);
        if(!gidEntryRow || !gidEntryRow?.rowId) return {error: "Missing gid entry!"}

        return await this.db.queryDatabase(
            `INSERT INTO inbox_profiles (gid_table_ref, icon, banner, signature, name)
             VALUES (?, ?, ?, ?, ?)
                 ON DUPLICATE KEY UPDATE
                                      updatedAt = (UNIX_TIMESTAMP() * 1000),
                                      name = COALESCE(VALUES(name), name),
                                      banner = COALESCE(VALUES(banner), banner),
                                      signature = COALESCE(VALUES(signature), signature),
                                      icon = COALESCE(VALUES(icon), icon)`,
            [gid_table_ref, icon, banner, signature, name]
        );
    }

    async init() {
        const tables = [
            {
                name: "inbox",
                columns: [
                    {name: "rowId", type: "int(100) NOT NULL AUTO_INCREMENT PRIMARY KEY"},
                    {name: "id", type: "varchar(255) NOT NULL UNIQUE KEY"},
                    {name: "customId", type: "varchar(500) NULL DEFAULT NULL"},
                    {name: "targetId", type: "varchar(500) NOT NULL"},
                    {name: "type", type: "varchar(255) NOT NULL"},
                    {name: "data", type: "longtext"},
                    {name: "createdAt", type: "bigint NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000)"},
                    {name: "expiresAt", type: "bigint NULL DEFAULT NULL"},
                    {name: "isRead", type: "bigint NULL DEFAULT NULL"},
                ]
            },
            {
                name: "inbox_gid_table",
                columns: [
                    {name: "rowId", type: "int(100) NOT NULL AUTO_INCREMENT PRIMARY KEY"},
                    {name: "vanity", type: "varchar(255) NULL DEFAULT NULL UNIQUE KEY"},
                    {name: "gid", type: "varchar(255) NOT NULL UNIQUE KEY"},
                    {name: "publicKey", type: "longtext"},
                    {name: "home_server", type: "varchar(500) NOT NULL"},
                    {name: "createdAt", type: "bigint NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000)"},
                    {name: "updatedAt", type: "bigint DEFAULT NULL"},
                ]
            },
            {
                name: "inbox_profiles",
                columns: [
                    {name: "rowId", type: "int(100) NOT NULL AUTO_INCREMENT PRIMARY KEY"},
                    {name: "gid_table_ref", type: "varchar(255) NOT NULL UNIQUE KEY"},
                    //
                    {name: "icon", type: "varchar(500) NULL DEFAULT NULL"},
                    {name: "banner", type: "varchar(500) NULL DEFAULT NULL"},
                    {name: "signature", type: "varchar(4000) NULL DEFAULT NULL"},
                    {name: "name", type: "varchar(255) NULL DEFAULT NULL"},
                    //
                    {name: "createdAt", type: "bigint NOT NULL DEFAULT (UNIX_TIMESTAMP() * 1000)"},
                    {name: "updatedAt", type: "bigint DEFAULT NULL"},
                ]
            }
        ]

        for (const table of tables) {
            await this.db.checkAndCreateTable(table);
        }
    }
}
