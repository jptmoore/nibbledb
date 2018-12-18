type t;
type datapoint;

let convert: list((int64, Ezjsonm.t)) => list((int64,datapoint));

let to_json: list((int64,datapoint)) => Ezjsonm.t;

let values: list((int64,datapoint)) => list(float);

let filter: (list((int64, datapoint)), (string, string) => bool, (string, string)) => list((int64, datapoint));

let print: (list((int64,datapoint))) => unit; 

let create: (~file: string, ~bare: bool) => Lwt.t(t);

let add: (Lwt.t(t), Irmin.Info.f, list(string), list((int64,datapoint))) => Lwt.t(unit);

let get: (Lwt.t(t), list(string)) => Lwt.t(list((int64,datapoint)));

let remove: (Lwt.t(t), Irmin.Info.f, list(list(string))) => Lwt.t(unit);


