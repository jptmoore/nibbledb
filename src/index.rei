type t;

let print: (list((int64, int64))) => unit; 

let create: (~file: string, ~bare: bool) => Lwt.t(t);

let update: (Lwt.t(t), Irmin.Info.f, string, (int64,int64), list((int64, int64))) => Lwt.t(option((int64,int64)));

let get: (Lwt.t(t), string) => Lwt.t(option(list((int64, int64))));

let overlap: (Lwt.t(t), string, (int64,int64)) => Lwt.t(list((int64, int64)));

let range: (Lwt.t(t), string) => Lwt.t(option((int64,int64)));