program mqttsubtcp;

{$APPTYPE CONSOLE}

{
  Helper: subscribe via TCP MQTT, wait for ONE message, print its payload, exit.

  Usage:
    mqtt-sub-tcp.exe <host> <port> <topic> [qos] [timeout_ms]

  Output (stdout):
    READY                <- printed once subscribed (parent can start publishing)
    MSG <payload>        <- printed when a message arrives

  Exit codes:
    0 = message received
    1 = bad args
    2 = connect/subscribe failed
    3 = timeout waiting for message
}

uses
  System.SysUtils,
  System.SyncObjs,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\..\src\MQTT.Logger.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

function MakeOptions: TMQTTConnectOptions;
begin
  Result.SetDefaults;
  Result.ClientID := 'tcp-sub-' + IntToStr(Random(1000000));
  Result.Version := MQTT311;
  Result.KeepAliveSec := 30;
end;

var
  Client: IMQTTClient;
  Host, Topic, Received: string;
  Port, QoSInt, TimeoutMs: Integer;
  QoS: TMQTTQoS;
  GotEvent: TEvent;
  WaitRes: TWaitResult;
begin
  try
    if ParamCount < 3 then
    begin
      Writeln('Usage: mqtt-sub-tcp.exe <host> <port> <topic> [qos] [timeout_ms]');
      Halt(1);
    end;

    Randomize;
    Host := ParamStr(1);
    Port := StrToInt(ParamStr(2));
    Topic := ParamStr(3);
    QoSInt := StrToIntDef(ParamStr(4), 1);
    TimeoutMs := StrToIntDef(ParamStr(5), 10000);
    QoS := TMQTTQoS(QoSInt);

    GotEvent := TEvent.Create(nil, True, False, '');
    try
      Client := CreateMQTTClient;
      try
        Client.Connect(Host, Port, MakeOptions);
      except
        on E: Exception do
        begin
          Writeln('ERROR connect: ', E.Message);
          Halt(2);
        end;
      end;

      try
        Client.Subscribe(Topic,
          procedure(const T: string; const P: TBytes)
          begin
            Received := TEncoding.UTF8.GetString(P);
            GotEvent.SetEvent;
          end,
          QoS);

        Sleep(500); // let SUBACK settle
        Writeln('READY');
        Flush(Output);

        WaitRes := GotEvent.WaitFor(TimeoutMs);
      finally
        Client.Disconnect;
      end;

      if WaitRes = wrSignaled then
      begin
        Writeln('MSG ', Received);
        Halt(0);
      end
      else
      begin
        Writeln('TIMEOUT');
        Halt(3);
      end;
    finally
      GotEvent.Free;
    end;
  except
    on E: Exception do
    begin
      Writeln('ERROR ', E.ClassName, ': ', E.Message);
      Halt(2);
    end;
  end;
end.
