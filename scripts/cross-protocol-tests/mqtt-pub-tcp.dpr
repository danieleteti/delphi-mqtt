program mqttpubtcp;

{$APPTYPE CONSOLE}

{
  Helper: publish one message via TCP MQTT and exit.

  Usage:
    mqtt-pub-tcp.exe <host> <port> <topic> <payload> [qos]

  Exit codes:
    0 = published OK (and ACKed for QoS>0)
    1 = bad args
    2 = connect/publish failed
}

uses
  System.SysUtils,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\..\src\MQTT.Logger.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

function MakeOptions: TMQTTConnectOptions;
begin
  Result.SetDefaults;
  Result.ClientID := 'tcp-pub-' + IntToStr(Random(1000000));
  Result.Version := MQTT311;
  Result.KeepAliveSec := 30;
end;

var
  Client: IMQTTClient;
  Host, Topic, Payload: string;
  Port, QoSInt: Integer;
  QoS: TMQTTQoS;
  Acked: Boolean;
begin
  try
    if ParamCount < 4 then
    begin
      Writeln('Usage: mqtt-pub-tcp.exe <host> <port> <topic> <payload> [qos]');
      Halt(1);
    end;

    Randomize;
    Host := ParamStr(1);
    Port := StrToInt(ParamStr(2));
    Topic := ParamStr(3);
    Payload := ParamStr(4);
    QoSInt := StrToIntDef(ParamStr(5), 1);
    QoS := TMQTTQoS(QoSInt);

    Client := CreateMQTTClient;
    Client.Connect(Host, Port, MakeOptions);
    try
      if QoS = atMostOnce then
      begin
        Client.Publish(Topic, Payload, QoS);
        Acked := True;
        Sleep(200); // let kernel flush
      end
      else
        Acked := Client.PublishSync(Topic, TEncoding.UTF8.GetBytes(Payload), QoS, False, 5000);
    finally
      Client.Disconnect;
    end;

    if Acked then
    begin
      Writeln('OK ', Topic);
      Halt(0);
    end
    else
    begin
      Writeln('NO-ACK');
      Halt(2);
    end;
  except
    on E: Exception do
    begin
      Writeln('ERROR ', E.ClassName, ': ', E.Message);
      Halt(2);
    end;
  end;
end.
