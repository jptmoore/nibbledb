type t;

let create: unit => t;

let write: (t, string, (int64, Ezjsonm.t)) => Lwt.t(unit);

let read: (t, string) => Lwt.t((int64, Ezjsonm.t));

let length: (t, string) => Lwt.t(int);

let exists: (t, string) => bool;

let to_list: (t, string) => Lwt.t(list((int64, Ezjsonm.t)));

let is_ascending: (t, string, int64) => bool;

let is_descending: (t, string, int64) => bool;

let serialise: t => Lwt.t(list((string, list((int64, Ezjsonm.t)))));

let empty: t => Lwt.t(unit);

let empty_series: (t, string) => Lwt.t(unit);

let set_disk_range: (t, string, option((int64, int64))) => unit;

let get_disk_range: (t, string) => option((int64, int64));