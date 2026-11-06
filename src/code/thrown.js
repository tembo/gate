// Capture intrinsics before the script can replace them. Formatting never calls
// getters, toJSON, or toString on a thrown object. Proxy traps can still run;
// the worker's normal deadline and memory limit apply to them.
(() => {
    const keys = Object.keys;
    const descriptor = Object.getOwnPropertyDescriptor;
    const isArray = Array.isArray;
    const stringify = JSON.stringify;
    const string = String;
    const slice = Function.prototype.call.bind(String.prototype.slice);
    return value => {
        let remaining = 2048;
        let output = "";
        const ancestors = [];
        const write = text => {
            const part = slice(text, 0, remaining);
            output += part;
            remaining -= part.length;
        };
        const quoted = text => stringify(slice(text, 0, 256) + (text.length > 256 ? "…" : ""));
        const visit = (value, depth) => {
            if (!remaining) return;
            if (typeof value === "string") { write(quoted(value)); return; }
            if (typeof value === "bigint") { write(string(value) + "n"); return; }
            if (typeof value === "function") { write("[Function]"); return; }
            if (value === null || typeof value !== "object") { write(string(value)); return; }
            for (let i = 0; i < ancestors.length; i++) {
                if (ancestors[i] === value) { write("[Circular]"); return; }
            }
            if (depth >= 4) { write("[Object]"); return; }
            ancestors[ancestors.length] = value;
            try {
                const array = isArray(value);
                const names = array ? null : keys(value);
                const count = array ? descriptor(value, "length").value : names.length;
                write(array ? "[" : "{");
                for (let i = 0; i < count && i < 20 && remaining; i++) {
                    if (i) write(", ");
                    if (!array) write(quoted(names[i]) + ": ");
                    const property = descriptor(value, array ? string(i) : names[i]);
                    if (!property) write(array ? "[Empty]" : "[Unavailable]");
                    else if ("value" in property) visit(property.value, depth + 1);
                    else write("[Getter]");
                }
                if (count > 20) write(", …");
                write(array ? "]" : "}");
            } catch {
                write("[Uninspectable object]");
            } finally {
                ancestors.length--;
            }
        };
        visit(value, 0);
        return output + (remaining === 0 ? "… [truncated]" : "");
    };
})();
