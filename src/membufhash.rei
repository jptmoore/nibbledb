type t;

let create: unit => t;

let add: (t, string, Membufq.t) => unit;

let replace: (t, string, Membufq.t) => unit;

let get: (t, string) => Membufq.t;

let exists: (t, string) => bool;

let get_keys: t => list(string);