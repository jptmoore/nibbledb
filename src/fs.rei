type t;

let create: (~index_dir: string) => t;

let ts_names: (~ctx: t) => Lwt.t(list(string));
