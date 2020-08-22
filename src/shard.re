open Lwt.Infix;

type datapoint = {
  tag: option(list((string,string))),
  value: float
};
  
let datapoint_t =
  Irmin.Type.(
    record("datapoint", (tag, value) => {tag, value})
    |+ field("tag", option(list(pair(string,string))), (t) => t.tag)
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
  /* let pp = Irmin.Type.pp_json(shard_t); */
  /* let of_string = (s) => Irmin.Type.decode_json(shard_t, Jsonm.decoder(`String(s))); */
};

module Store = Irmin_unix.Git.FS.KV(Shard);

type t = Store.t;
type err = Store.write_error;

let make_json_tag = (data) => {
  open List;
  let rec loop = (acc, l) => {
    switch (l) {
    | `A([]) => rev(acc);
    | `A([`O([(s1, `String(s2))]), ...rest]) => loop(cons((s1,s2), acc), `A(rest));
    | _ => failwith("badly formatted json");
    }
  };
  loop([], data);    
}

let make_native_tag = (data) => {
  open List;
  let rec loop = (acc, l) => {
    switch (l) {
    | [] => `A(rev(acc));
    | [(name,value), ...rest] => 
        loop(cons(Ezjsonm.dict([(name, `String(value))]), acc), rest);
    }
  };
  loop([], data);  
}

let format_datapoint = (ts, tag, value) => {
  (ts,{tag: tag, value: value});
};


let convert_worker = (ts, datapoint) => {
  open Ezjsonm;
  switch(datapoint) {
  | [(_, n)] =>
      format_datapoint(ts, None, get_float(n));
  | [("tag", tag), (_, n)] => 
      format_datapoint(ts, Some(make_json_tag(tag)), get_float(n));  
  | _ => failwith("badly formatted json");
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
      | Some(t) => dict([("timestamp", int64(ts)), ("tag", make_native_tag(t)), ("data", dict([("value", float(v))]))]);
      | None => dict([("timestamp", int64(ts)), ("value", float(v))]);
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


let filter_worker_helper = (name, value, tagset, func) => {
  switch (List.assoc_opt(name, tagset)) {
    | None => false
    | Some(value') => func(value, value')
  }
}

let filter_worker = (data, func, name, value) => {
  List.filter(((_,dp)) => switch (dp.tag) {
  | None => false;
  | Some(tagset) => filter_worker_helper(name, value, tagset, func)
  }, data)
};

let filter = (data, func, tag) => {
  let (name_set, value_set) = tag;
  let names = String.split_on_char(',', name_set); 
  let values = String.split_on_char(',', value_set); 
  List.length(names) != List.length(values) ? failwith("invalid filter format") : ()
  List.fold_left2((acc, name, value) => 
    filter_worker(data, func, name, value) |>
      List.rev_append(acc), [], names, values);
}

let create = (~file, ~bare) => {
  let config = Irmin_git.config(file, ~bare);
  let repo = Store.Repo.v(config);
  repo >>= (repo => Store.master(repo));
};

let add = (branch, info, k, v) => {
  branch >>= (branch' => Store.set_exn(branch', ~info, k, v));
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



