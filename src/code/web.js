// Captured host functions expose no file, network, or process operations.
(() => {
    const startTimer = globalThis.__gateStartTimer;
    const waitTimer = globalThis.__gateWaitTimer;
    const clearTimer = globalThis.__gateClearTimer;
    const parseURL = globalThis.__gateURL;
    const parseQuery = globalThis.__gateQueryParse;
    const encodeQuery = globalThis.__gateQueryEncode;
    for (const name of ["__gateStartTimer", "__gateWaitTimer", "__gateClearTimer", "__gateURL", "__gateQueryParse", "__gateQueryEncode"]) delete globalThis[name];

    let nextTimer = 0;
    const timers = new Map();
    const schedule = (repeat, callback, delay, args) => {
        if (typeof callback !== "function") throw new TypeError("timer callback must be a function");
        if (timers.size >= 256) throw new RangeError("at most 256 timers may be pending");
        let ms = Number(delay);
        ms = !Number.isFinite(ms) || ms < 1 || ms > 2147483647 ? 1 : Math.trunc(ms);
        // IDs are not reused while asynchronous cancellation is still settling.
        if (nextTimer === 0xffffffff) throw new RangeError("timer ID limit reached");
        const id = ++nextTimer;
        timers.set(id, callback);
        const tick = () => {
            startTimer(id);
            waitTimer(id, ms).then(fired => {
                if (!fired || !timers.has(id)) return;
                if (!repeat) timers.delete(id);
                callback(...args);
                if (repeat && timers.has(id)) tick();
            });
        };
        tick();
        return id;
    };
    globalThis.setTimeout = (callback, delay = 0, ...args) => schedule(false, callback, delay, args);
    globalThis.setInterval = (callback, delay = 0, ...args) => schedule(true, callback, delay, args);
    globalThis.clearTimeout = globalThis.clearInterval = id => {
        if (timers.delete(id)) clearTimer(id);
    };

    // Web strings replace unpaired UTF-16 surrogates with U+FFFD.
    const scalarString = input => String(input).toWellFormed();
    class URLSearchParams {
        #pairs;
        #changed;
        constructor(init = "") {
            if (typeof init === "string") this.#pairs = JSON.parse(parseQuery(scalarString(init).replace(/^\?/, "")));
            else if (init != null && typeof init[Symbol.iterator] === "function") {
                this.#pairs = Array.from(init, pair => {
                    if (pair === null || typeof pair !== "object" || typeof pair[Symbol.iterator] !== "function") throw new TypeError("query entries must be pairs");
                    const values = Array.from(pair);
                    if (values.length !== 2) throw new TypeError("query entries need two values");
                    return values.map(scalarString);
                });
            } else this.#pairs = Object.entries(init ?? {}).map(pair => pair.map(scalarString));
        }
        static attach(owner) {
            const params = new URLSearchParams(owner.search);
            params.#changed = () => owner.search = params.toString();
            return params;
        }
        static refresh(params, search) { params.#pairs = JSON.parse(parseQuery(search.replace(/^\?/, ""))); }
        get size() { return this.#pairs.length; }
        append(name, value) { this.#pairs.push([scalarString(name), scalarString(value)]); this.#changed?.(); }
        delete(name, value) {
            name = scalarString(name);
            if (value !== undefined) value = scalarString(value);
            this.#pairs = this.#pairs.filter(pair => pair[0] !== name || (value !== undefined && pair[1] !== value));
            this.#changed?.();
        }
        get(name) { return this.#pairs.find(pair => pair[0] === scalarString(name))?.[1] ?? null; }
        getAll(name) { name = scalarString(name); return this.#pairs.filter(pair => pair[0] === name).map(pair => pair[1]); }
        has(name, value) {
            name = scalarString(name);
            if (value !== undefined) value = scalarString(value);
            return this.#pairs.some(pair => pair[0] === name && (value === undefined || pair[1] === value));
        }
        set(name, value) {
            name = scalarString(name); value = scalarString(value);
            let found = false;
            this.#pairs = this.#pairs.filter(pair => {
                if (pair[0] !== name) return true;
                if (found) return false;
                pair[1] = value; found = true; return true;
            });
            if (!found) this.#pairs.push([name, value]);
            this.#changed?.();
        }
        sort() { this.#pairs.sort((a, b) => a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0); this.#changed?.(); }
        *entries() { for (let i = 0; i < this.#pairs.length; i++) yield [...this.#pairs[i]]; }
        *keys() { for (const pair of this.entries()) yield pair[0]; }
        *values() { for (const pair of this.entries()) yield pair[1]; }
        [Symbol.iterator]() { return this.entries(); }
        forEach(callback, thisArg) { for (const [key, value] of this.entries()) callback.call(thisArg, value, key, this); }
        toString() { return encodeQuery(JSON.stringify(this.#pairs)); }
        get [Symbol.toStringTag]() { return "URLSearchParams"; }
    }
    class URL {
        #parts;
        #params;
        constructor(input, base = undefined) {
            this.#parts = JSON.parse(parseURL(scalarString(input), base === undefined ? undefined : scalarString(base), undefined, undefined));
            this.#params = URLSearchParams.attach(this);
        }
        #set(field, value) {
            this.#parts = JSON.parse(parseURL(this.href, undefined, field, scalarString(value)));
            URLSearchParams.refresh(this.#params, this.search);
        }
        get href() { return this.#parts.href; } set href(value) { this.#set("href", value); }
        get origin() { return this.#parts.origin; }
        get protocol() { return this.#parts.protocol; } set protocol(value) { this.#set("protocol", value); }
        get username() { return this.#parts.username; } set username(value) { this.#set("username", value); }
        get password() { return this.#parts.password; } set password(value) { this.#set("password", value); }
        get host() { return this.#parts.host; } set host(value) { this.#set("host", value); }
        get hostname() { return this.#parts.hostname; } set hostname(value) { this.#set("hostname", value); }
        get port() { return this.#parts.port; } set port(value) { this.#set("port", value); }
        get pathname() { return this.#parts.pathname; } set pathname(value) { this.#set("pathname", value); }
        get search() { return this.#parts.search; } set search(value) { this.#set("search", value); }
        get hash() { return this.#parts.hash; } set hash(value) { this.#set("hash", value); }
        get searchParams() { return this.#params; }
        toString() { return this.href; }
        toJSON() { return this.href; }
        static canParse(input, base) { try { new URL(input, base); return true; } catch { return false; } }
        static parse(input, base) { try { return new URL(input, base); } catch { return null; } }
        get [Symbol.toStringTag]() { return "URL"; }
    }
    class TextEncoder {
        get encoding() { return "utf-8"; }
        encode(input = "") {
            input = String(input);
            const output = new Uint8Array(input.length * 3);
            const { written } = this.encodeInto(input, output);
            return output.slice(0, written);
        }
        encodeInto(input, destination) {
            if (!(destination instanceof Uint8Array)) throw new TypeError("destination must be a Uint8Array");
            input = String(input);
            let read = 0, written = 0;
            while (read < input.length) {
                let point = input.codePointAt(read);
                const units = point > 0xffff ? 2 : 1;
                if (point >= 0xd800 && point <= 0xdfff) point = 0xfffd;
                const count = point < 0x80 ? 1 : point < 0x800 ? 2 : point < 0x10000 ? 3 : 4;
                if (destination.length - written < count) break;
                if (count === 1) destination[written++] = point;
                else {
                    destination[written++] = (count === 2 ? 0xc0 : count === 3 ? 0xe0 : 0xf0) | (point >> (6 * (count - 1)));
                    for (let shift = 6 * (count - 2); shift >= 0; shift -= 6) destination[written++] = 0x80 | ((point >> shift) & 0x3f);
                }
                read += units;
            }
            return { read, written };
        }
        get [Symbol.toStringTag]() { return "TextEncoder"; }
    }
    Object.assign(globalThis, { URL, URLSearchParams, TextEncoder });
})();
