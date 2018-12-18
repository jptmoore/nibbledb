open Lwt.Infix;

type datapoint = {
  tag: option((string,string)),
  value: float
};
  
  
let datapoint_t =
  Irmin.Type.(
    record("datapoint", (tag, value) => {tag, value})
    |+ field("tag", option(pair(string,string)), (t) => t.tag)
    |+ field("value", float, (t) => t.value)
    |> sealr
);
  
type shard = list((int64,datapoint));

let shard_t = Irmin.Type.(list(pair(int64, datapoint_t)));

  
let print = (m) => Fmt.pr("%a\n%!", Irmin.Type.pp_json(shard_t), m);
  
let merge = Irmin.Merge.(option(idempotent(shard_t)));


module Shard: Irmin.Contents.S with type t = shard = {
  type t = shard;
  let t = shard_t;
  let merge = merge;
  let pp = Irmin.Type.pp_json(shard_t);
  let of_string = (s) => Irmin.Type.decode_json(shard_t, Jsonm.decoder(`String(s)));
};

module Store = Irmin_unix.Git.FS.KV(Shard);

type t = Store.t;
type contents = Store.contents;

let format_datapoint = (ts, tag, value) => {
  (ts,{tag: tag, value: value});
};

let convert_worker = (ts, datapoint) => {
  open Ezjsonm;
  switch(datapoint) {
  | [(_, n)] => 
      format_datapoint(ts, None, get_float(n));
  | [(s1,s2), (_, n)] => 
      format_datapoint(ts, Some((s1, get_string(s2))), get_float(n));
  | _ => failwith("Error:badly formatted JSON");
  }
};

let convert = (data) => {
  open List;
  let rec loop = (acc, l) => {
    switch (l) {
    | [] => rev(acc);
    | [ (ts, json), ...rest] => {
          let dp = Ezjsonm.get_dict(Ezjsonm.value(json));
          loop(cons(convert_worker(ts, dp), acc), rest);
        };
    }
  };
  loop([], data);
};


let to_json_worker = (ts, datapoint) => {
  open Ezjsonm;
  switch(datapoint) {
  | {tag: t, value: v} => {
      switch(t) {
      | Some((s1,s2)) => dict([("timestamp", int64(ts)), ("data", dict([(s1, string(s2)), ("value", float(v))]))]);
      | None => dict([("timestamp", int64(ts)), ("data", dict([("value", float(v))]))]);
      }
    }
  }
};

let to_json = (data) => {
  Ezjsonm.list(((ts,dp)) => to_json_worker(ts,dp), data);
};

let values = (data) => {
  List.rev_map(((_,dp)) => dp.value, data);
};

let filter = (data, func, tag) => {
  let (name, value) = tag;
  List.filter(((_,dp)) => switch (dp.tag) {
  | None => false;
  | Some((name',value')) => (name == name') && (func(value, value')) 
  }, data)
};

let create = (~file, ~bare) => {
  let config = Irmin_git.config(file, ~bare);
  let repo = Store.Repo.v(config);
  repo >>= (repo => Store.master(repo));
};

let add = (branch, info, k, v) => {
  branch >>= (branch' => Store.set(branch', ~info, k, v));
};

let sort_shard = (lis) => {
  let cmp = ((x, _), (x', _)) => x < x' ? 1 : (-1);
  List.sort(cmp, lis);
};

let get = (branch, k) => {
  branch >>= (branch' => Store.get(branch', k)) >|= sort_shard;
};

let remove = (branch, info, key_list) => {
  Lwt_list.iter_s(k => add(branch, info, k, []), key_list);
};



