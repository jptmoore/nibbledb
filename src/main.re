open Lwt.Infix;

open Cohttp;

open Cohttp_lwt_unix;

let path_to_db = ref("/tmp/nibble/");
let http_port = ref(8000);
let max_buffer_size = ref(10000);
let shard_size = ref(1000);
let show_files = ref(false);
let log_mode = ref(false);
let tls_mode = ref(false);
let cert_file = ref("/tmp/server.crt");
let key_file = ref("/tmp/server.key");

let info = (fmt) => Irmin_unix.info(~author="nibbledb", fmt);

type t = {
  db: Timeseries.t,
  m: Lwt_mutex.t
};

exception Interrupt(string);


module Http_response {
  let json_header = Header.of_list([("Content-Type", "application/json")]);
  let text_header = Header.of_list([("Content-Type", "text/plain")]);
  let ok = (~content="", ()) => {
    Server.respond_string(~status=`OK, ~body=content, ~headers=json_header, ());
  };
  let bad_request = (~content="", ()) => {
    Server.respond_string(~status=`Bad_request, ~body=content, ~headers=text_header, ());
  };
  let not_found = (~content="", ()) => {
    Server.respond_string(~status=`Not_found, ~body=content, ~headers=text_header, ());
  };
  let internal_server_error = (~content="", ()) => {
    Server.respond_string(~status=`Internal_server_error, ~body=content, ~headers=text_header, ());
  };
};

let post_worker = (ctx, id, json) => {
  open Timeseries;
  switch(validate_json(json)) {
  | Some((t,j)) => write(~ctx=ctx.db, ~info=info("write"), ~timestamp=t, ~id=id, ~json=j)
  | None => failwith("Error:badly formatted JSON\n")
  };
}

let post = (ctx, id, body) => {
  open Timeseries;
  open Ezjsonm;
  body |> Cohttp_lwt.Body.to_string >|=
    Ezjsonm.from_string >>= json => switch(json) {
    | `O(_) => post_worker(ctx, id, json) 
    | `A(lis) => Lwt_list.iter_s(x => post_worker(ctx, id, `O(get_dict(x))), lis)
    } >>= fun () => Http_response.ok()
};

let post_req = (ctx, path_list, body) => {
  switch (path_list) {
  | [_, _, _, "ts", id] => post(ctx, id, body)
  | _ => failwith("Error:unknown path\n")
  }
};

let read_last = (ctx, ids, n, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_last(~ctx=ctx.db, ~info=info("read_last"), ~id_list, ~n=int_of_string(n), ~xargs) >|=
    Ezjsonm.to_string >>= s => Http_response.ok(~content=s, ()) 
};

let read_first = (ctx, ids, n, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_first(~ctx=ctx.db, ~info=info("read_first"), ~id_list, ~n=int_of_string(n), ~xargs) >|=
    Ezjsonm.to_string >>= s => Http_response.ok(~content=s, ()) 
};

let read_since = (ctx, ids, from, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_since(~ctx=ctx.db, ~info=info("read_since"), ~id_list, ~from=Int64.of_string(from), ~xargs) >|=
    Ezjsonm.to_string >>= s => Http_response.ok(~content=s, ()) 
};

let delete_since = (ctx, ids, from, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_since(~ctx=ctx.db, ~info=info("read_since"), ~id_list, ~from=Int64.of_string(from), ~xargs) >>=
    json => delete(~ctx=ctx.db, ~info=info("delete_since"), ~id_list, ~json) >>= 
      () => Http_response.ok() 
};

let read_range = (ctx, ids, from, to_, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_range(~ctx=ctx.db, ~info=info("read_range"), ~id_list, ~from=Int64.of_string(from), ~to_=Int64.of_string(to_), ~xargs) >|=
    Ezjsonm.to_string >>= s => Http_response.ok(~content=s, ()) 
};

let delete_range = (ctx, ids, from, to_, xargs) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  read_range(~ctx=ctx.db, ~info=info("read_range"), ~id_list, ~from=Int64.of_string(from), ~to_=Int64.of_string(to_), ~xargs) >>=
    json => delete(~ctx=ctx.db, ~info=info("delete_range"), ~id_list, ~json) >>=
      () => Http_response.ok()
};

let length = (ctx, ids) => {
  open Timeseries;
  let id_list = String.split_on_char(',', ids);
  length(~ctx=ctx.db, ~id_list) >>=
    n => Http_response.ok(~content=Printf.sprintf("{\"length\":%d}", n), ()) 
}

let get_req = (ctx, path_list) => {
  switch (path_list) {
  | [_, _, _, "ts", ids, "last", n, ...xargs] => read_last(ctx, ids, n, xargs)
  | [_, _, _, "ts", ids, "latest", ...xargs] => read_last(ctx, ids, "1", xargs)
  | [_, _, _, "ts", ids, "first", n, ...xargs] => read_first(ctx, ids, n, xargs)
  | [_, _, _, "ts", ids, "earliest", ...xargs] => read_first(ctx, ids, "1", xargs)
  | [_, _, _, "ts", ids, "since", from, ...xargs] => read_since(ctx, ids, from, xargs)
  | [_, _, _, "ts", ids, "range", from, to_, ...xargs] => read_range(ctx, ids, from, to_, xargs)
  | [_, _, _, "ts", ids, "length"] => length(ctx, ids)
  | _ => Http_response.bad_request(~content="Error:unknown path\n", ())
  }
};

let delete_req = (ctx, path_list) => {
  switch (path_list) {
  | [_, _, _, "ts", ids, "since", from, ...xargs] => delete_since(ctx, ids, from, xargs)
  | [_, _, _, "ts", ids, "range", from, to_, ...xargs] => delete_range(ctx, ids, from, to_, xargs)
  | _ => Http_response.bad_request(~content="Error:unknown path\n", ())
  }
};

let handle_req_worker = (ctx, req, body) => {
  let meth = req |> Request.meth;
  let uri_path = req |> Request.uri |> Uri.to_string;
  let path_list = String.split_on_char('/', uri_path);
  switch (meth) {
  | `POST => post_req(ctx, path_list, body);
  | `GET => get_req(ctx, path_list);
  | `DELETE => delete_req(ctx, path_list);
  | _ => Http_response.bad_request(~content="Error:unknown method\n", ())
  }
};

let handle_req_safe = (ctx, req, body) => {
  () => Lwt.catch(
    () => handle_req_worker(ctx, req, body),
    fun
    | Failure(m) => Http_response.bad_request(~content=Printf.sprintf("Error:%s\n",m), ())
    | e => Lwt.fail(e)
  );
};

let handle_req = (ctx, req, body) => {
  Lwt_mutex.with_lock(ctx.m, handle_req_safe(ctx, req, body))
};


let server (~ctx) = {
  let callback = (_conn, req, body) => handle_req(ctx, req, body);
  let http = `TCP(`Port(http_port^));
  let https = `TLS((`Crt_file_path(cert_file^), `Key_file_path(key_file^), `No_password, `Port(http_port^)));
  Server.create(~mode=(tls_mode^ ? https : http), Server.make(~callback, ()));
};


let register_signal_handlers = () => {
  Lwt_unix.(on_signal(Sys.sigterm, (_) => raise(Interrupt("Caught SIGTERM"))) |> 
    id => on_signal(Sys.sighup, (_) => raise(Interrupt("Caught SIGHUP"))) |> 
      id => on_signal(Sys.sigint, (_) => raise(Interrupt("Caught SIGINT"))));
};

let parse_cmdline = () => {
  let usage = "usage: " ++ Sys.argv[0];
  let speclist = [
    (
      "--path-to-db",
      Arg.Set_string(path_to_db),
      ": to set the location for the database files"
    ),
    (
      "--cert-file",
      Arg.Set_string(cert_file),
      ": to provide the TLS certificate"
    ),
    (
      "--key-file",
      Arg.Set_string(key_file),
      ": to provide the TLS key"
    ),
    (
      "--http-port",
      Arg.Set_int(http_port),
      ": to set the http port"
    ), 
    (
      "--max-buffer-size",
      Arg.Set_int(max_buffer_size),
      ": to set the max buffer size"
    ), 
    (
      "--shard-size",
      Arg.Set_int(shard_size),
      ": to set the shard size"
    ),
    (
      "--show-files",
      Arg.Set(show_files),
      ": to show files in git"
    ),
    ("--enable-debug", Arg.Set(log_mode), ": turn debug mode on"), 
    ("--enable-tls", Arg.Set(tls_mode), ": use https") 

  ];
  Arg.parse(speclist, x => raise(Arg.Bad("Bad argument : " ++ x)), usage);
};

let enable_debug = () => {
  Lwt_log_core.default :=
    Lwt_log.channel(
      ~template="$(date).$(milliseconds) [$(level)] $(message)",
      ~close_mode=`Keep,
      ~channel=Lwt_io.stdout,
      ()
    );
  Lwt_log_core.add_rule("*", Lwt_log_core.Debug);
};

let init = () => {
  let () = ignore(register_signal_handlers());
  parse_cmdline();
  log_mode^ ? enable_debug() : ();
  { 
    db: Timeseries.create(~path_to_db=path_to_db^, ~max_buffer_size=max_buffer_size^, ~shard_size=shard_size^, ~show_files=show_files^), 
    m: Lwt_mutex.create()
  };
};

let flush_server = (ctx) => {
  Lwt_main.run {
    Lwt_io.printf("\nShutting down server...\n") >>=
      () => Timeseries.flush(~ctx=ctx.db, ~info=info("flush")) >>=
        () => Lwt_unix.sleep(1.0) >>=
          () => Lwt_io.printf("OK\n")
  };
};

let run_server = (~ctx) => {
  let () = {
    try (Lwt_main.run(server(ctx))) {
    | Interrupt(m) => ignore(flush_server(ctx));
    };
  };
};

run_server(~ctx=init());

