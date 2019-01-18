open Lwt;

open Cohttp;

open Cohttp_lwt_unix;

let uri = ref("http://127.0.0.1:8000/ts/foo");

let content_format = ref("json");

let payload = ref("{\"value\": 42}");

let loop_count = ref(0);

let call_freq = ref(1.0);

let file = ref(false);

let send_request = (~uri, ~payload) => {
  let headers = Cohttp.Header.of_list([("Content-Type", content_format^), ("Connection", "keep-alive")]);
  let body = Cohttp_lwt.Body.of_string(payload);
  Client.post(~headers=headers, ~body=body, Uri.of_string(uri)) >>= 
    (((_, body)) => body |> Cohttp_lwt.Body.to_string);
};

let post_loop = (count) => {
  let rec loop = (n) =>
    send_request(~uri=uri^, ~payload=payload^) >>=
      (resp) => Lwt_io.printf("=> Created\n") >>= () => 
        if (n > 1) {
          Lwt_unix.sleep(call_freq^) >>= () => loop(n - 1);
        } else {
          Lwt.return_unit;
        }
  loop(count);
};

let post_test = () => post_loop(loop_count^);

let handle_format = (format) => {
  let format =
    switch format {
    | "text" => "text/plain"
    | "json" => "application/json"
    | "binary" => "application/octet-stream"
    | _ => raise(Arg.Bad("Unsupported format"))
    };
  content_format := format;
};

let parse_cmdline = () => {
  let usage = "usage: " ++ Sys.argv[0];
  let speclist = [
    ("--uri", Arg.Set_string(uri), ": to set the uri"),
    ("--payload", Arg.Set_string(payload), ": to set the message payload"),
    (
      "--format",
      Arg.Symbol(["text", "json", "binary"], handle_format),
      ": to set the message content type"
    ),
    (
      "--loop",
      Arg.Set_int(loop_count),
      ": to set the number of times to run post/get/observe test"
    ),
    (
      "--freq",
      Arg.Set_float(call_freq),
      ": to set the number of seconds to wait between each get/post operation"
    ),
    ("--file", Arg.Set(file), ": payload contents comes from a file")
  ];
  Arg.parse(speclist, (err) => raise(Arg.Bad("Bad argument : " ++ err)), usage);
};

let set_payload_from = (file) => {
  let data = Fpath.v(file) |> Bos.OS.File.read |> Rresult.R.get_ok;
  payload := data;
};

parse_cmdline();

file^ ? set_payload_from(payload^) : ();

Lwt_main.run(post_test());