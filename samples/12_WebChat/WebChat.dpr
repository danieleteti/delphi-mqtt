program WebChat;

{$APPTYPE CONSOLE}

(*
  WebChat - Delphi <-> Browser chat sample

  Console MQTT chat client that participates in a 3-way conversation with two
  HTML pages running on the same Mosquitto broker.

  Topology:
    * Mosquitto on localhost
        - TCP listener     : 1883 (Delphi client connects here)
        - WebSocket listener: 9001 (browser pages connect here, path /mqtt)
    * All clients subscribe to:    samples/webchat/messages
    * Each client also publishes a presence message (retained) on:
                                   samples/webchat/presence/<id>

  Message payload is JSON like:
      {"from": "user", "text": "hi", "ts": "iso8601"}

  Run:
    1. dcc64 WebChat.dpr -U"..\..\src"
    2. WebChat.exe                  -> Delphi side
    3. python serve.py              -> serves /web/ on http://localhost:8080
       Open http://localhost:8080/alice.html and bob.html in two browsers
*)

uses
  System.SysUtils,
  System.Classes,
  System.JSON,
  System.DateUtils,
  System.SyncObjs,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\..\src\MQTT.Logger.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

const
  TOPIC_MESSAGES  = 'samples/webchat/messages';
  TOPIC_PRESENCE  = 'samples/webchat/presence/';
  BROKER_HOST     = 'localhost';
  BROKER_PORT     = 1883;

var
  ConsoleLock: TCriticalSection;
  UserName: string;
  Client: IMQTTClient;

function NowIso: string;
begin
  Result := FormatDateTime('yyyy-mm-dd"T"hh:nn:ss.zzz', Now);
end;

procedure SafeWriteln(const S: string);
begin
  ConsoleLock.Enter;
  try
    Writeln(S);
  finally
    ConsoleLock.Leave;
  end;
end;

procedure HandleIncoming(const Topic: string; const Payload: TBytes);
var
  Json: TJSONObject;
  FromUser, MsgText, Ts, Raw: string;
  V: TJSONValue;
begin
  Raw := TEncoding.UTF8.GetString(Payload);

  // presence retained message?
  if Topic.StartsWith(TOPIC_PRESENCE) then
  begin
    if Raw = '' then
      SafeWriteln(Format('[presence] %s left', [Topic.Substring(Length(TOPIC_PRESENCE))]))
    else
      SafeWriteln(Format('[presence] %s', [Raw]));
    Exit;
  end;

  FromUser := '?';
  MsgText := '';
  Ts := '';
  Json := nil;
  try
    Json := TJSONObject.ParseJSONValue(Raw) as TJSONObject;
    if Json = nil then
    begin
      SafeWriteln(Format('[raw] %s -> %s', [Topic, Raw]));
      Exit;
    end;

    V := Json.GetValue('from');
    if V is TJSONString then FromUser := TJSONString(V).Value;
    V := Json.GetValue('text');
    if V is TJSONString then MsgText := TJSONString(V).Value;
    V := Json.GetValue('ts');
    if V is TJSONString then Ts := TJSONString(V).Value;

    if SameText(FromUser, UserName) then
      Exit;

    SafeWriteln(Format('[%s] %s: %s', [Ts, FromUser, MsgText]));
  finally
    Json.Free;
  end;
end;

procedure PublishChat(const Text: string);
var
  Json: TJSONObject;
  S: string;
begin
  Json := TJSONObject.Create;
  try
    Json.AddPair('from', UserName);
    Json.AddPair('text', Text);
    Json.AddPair('ts', NowIso);
    S := Json.ToString;
  finally
    Json.Free;
  end;
  Client.Publish(TOPIC_MESSAGES, S, atLeastOnce);
end;

procedure PublishPresence(const StatusText: string);
var
  Json: TJSONObject;
  S, Topic: string;
begin
  Json := TJSONObject.Create;
  try
    Json.AddPair('from', UserName);
    Json.AddPair('status', StatusText);
    Json.AddPair('ts', NowIso);
    S := Json.ToString;
  finally
    Json.Free;
  end;
  Topic := TOPIC_PRESENCE + UserName;
  Client.Publish(Topic, S, atLeastOnce, True); // retained
end;

procedure PublishOffline;
var
  Topic: string;
begin
  // empty retained payload clears the presence message
  Topic := TOPIC_PRESENCE + UserName;
  try
    Client.Publish(Topic, '', atLeastOnce, True);
  except
  end;
end;

var
  Options: TMQTTConnectOptions;
  Input: string;
begin
  ConsoleLock := TCriticalSection.Create;
  try
    Writeln('===========================================');
    Writeln('  Delphi MQTT - WebChat (Delphi <-> Web)');
    Writeln('===========================================');
    Writeln('Broker: ', BROKER_HOST, ':', BROKER_PORT, ' (TCP)');
    Writeln('Topic:  ', TOPIC_MESSAGES);
    Writeln;
    Write('Your name: ');
    Readln(UserName);
    UserName := Trim(UserName);
    if UserName = '' then
      UserName := 'delphi-' + IntToStr(Random(10000));

    try
      Client := CreateMQTTClient;
      Client.Logger := CreateNullLogger; // silence MQTT internals; chat output owns stdout

      Options.SetDefaults;
      Options.ClientID := 'delphi-' + UserName + '-' + IntToStr(Random(100000));
      Options.Version := MQTT311;
      Options.KeepAliveSec := 30;
      Options.CleanStart := True;
      // last will: announce offline if we crash
      Options.Will.Enabled := True;
      Options.Will.Topic := TOPIC_PRESENCE + UserName;
      Options.Will.Payload := TEncoding.UTF8.GetBytes(''); // clears retained
      Options.Will.QoS := atLeastOnce;
      Options.Will.Retain := True;

      Client.Connect(BROKER_HOST, BROKER_PORT, Options);
      Client.Subscribe(TOPIC_MESSAGES, HandleIncoming, atLeastOnce);
      Client.Subscribe(TOPIC_PRESENCE + '+', HandleIncoming, atLeastOnce);

      PublishPresence('online');
      SafeWriteln(Format('Logged in as "%s". Type a message and press Enter. Empty line + Enter to quit.', [UserName]));
      SafeWriteln('-------------------------------------------------------------');

      while True do
      begin
        Readln(Input);
        if Trim(Input) = '' then
          Break;
        PublishChat(Trim(Input));
      end;

      PublishOffline;
      Sleep(200);
      Client.Disconnect;
    except
      on E: Exception do
        Writeln('Error: ', E.ClassName, ': ', E.Message);
    end;
  finally
    ConsoleLock.Free;
  end;
end.
