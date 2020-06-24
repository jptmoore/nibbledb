type t;

let create: (~path_to_db: string, ~max_buffer_size: int, ~shard_size: int, ~show_files: bool) => t;

let validate_json: Ezjsonm.t => option((int64, Ezjsonm.t));

let write: (~ctx: t, ~info: Irmin.Info.f, ~timestamp: int64, ~id: string, ~json: Ezjsonm.t) => Lwt.t(unit);

let flush: (~ctx: t, ~info: Irmin.Info.f) => Lwt.t(unit);

let length: (~ctx: t, ~id_list: list(string)) => Lwt.t(int);

let length_in_memory: (~ctx: t, ~id_list: list(string)) => Lwt.t(int);

let length_on_disk: (~ctx: t, ~id_list: list(string)) => Lwt.t(int);

let length_of_index: (~ctx: t, ~id_list: list(string)) => Lwt.t(int);

let get_index: (~ctx: t, ~id: string) => Lwt.t(Ezjsonm.t);

let delete: (~ctx: t, ~info: Irmin.Info.f, ~id_list: list(string), ~json: Ezjsonm.t) => Lwt.t(unit);

let read_last: (~ctx: t, ~info: Irmin.Info.f, ~id_list: list(string), ~n: int, ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let read_latest: (~ctx: t, ~info: Irmin.Info.f, ~id_list: list(string), ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let read_first: (~ctx: t, ~info: Irmin.Info.f, ~id_list: list(string), ~n: int, ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let read_earliest: (~ctx: t, ~info: Irmin.Info.f, ~id_list: list(string), ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let read_since: (~ctx: t, ~id_list: list(string), ~from: int64, ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let read_range: (~ctx: t, ~id_list: list(string), ~from: int64, ~to_: int64, ~xargs: list(string)) => Lwt.t(Ezjsonm.t);

let names: (~ctx: t) => Lwt.t(Ezjsonm.t);

let stats: (~ctx: t) => Lwt.t(Ezjsonm.t);
