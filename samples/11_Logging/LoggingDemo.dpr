program LoggingDemo;

{$APPTYPE CONSOLE}

(*
  Logging interface + packet monitoring + SUBACK event sample.

  This sample uses the trivial in-process logger (CreateProcLogger) provided
  by MQTT.Logger. It is meant to show the IMQTTLogger contract and how to
  bridge it to any callback you want.

  For production logging through LoggerPro, see samples/13_LoggerPro/.

  Requirements: MQTT broker on localhost:1883.
*)

uses
  System.SysUtils,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\..\src\MQTT.Logger.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  PacketCounter: Integer;

begin
  PacketCounter := 0;
  try
    Writeln('Delphi MQTT - Logging + Monitoring Sample');
    Writeln('------------------------------------------');
    Writeln;

    Client := CreateMQTTClient;

    // Trivial inline logger: prints "[LEVEL] msg" to stdout. Shows the contract.
    Client.Logger := CreateProcLogger(
      procedure(Level: TMQTTLogLevel; const Msg: string)
      begin
        Writeln(Format('[%-5s] %s', [LogLevelName(Level), Msg]));
      end,
      llDebug);

    // P3: per-packet inbound hook
    Client.SetOnPacketReceived(
      procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin
        Inc(PacketCounter);
      end);

    // P3: per-packet outbound hook
    Client.SetOnPacketSent(
      procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin
        Inc(PacketCounter);
      end);

    // P4: SUBACK confirmation with granted QoS array
    Client.SetOnSubscribeAck(
      procedure(PacketID: Word; const GrantedQoS: TArray<Byte>)
      var
        I: Integer;
      begin
        Write('[APP  ] SUBACK PacketID=', PacketID, ' granted: ');
        for I := 0 to High(GrantedQoS) do
        begin
          if I > 0 then Write(',');
          if GrantedQoS[I] >= $80 then
            Write('FAIL(', GrantedQoS[I], ')')
          else
            Write('QoS', GrantedQoS[I]);
        end;
        Writeln;
      end);

    Options.SetDefaults;
    Options.ClientID := 'LoggingDemo';
    Options.Version := MQTT5;

    Client.Connect('localhost', 1883, Options);

    Client.Subscribe('demo/logging/+',
      procedure(const Topic: string; const Payload: TBytes)
      begin
        Writeln('[APP  ] message on ', Topic, ': ', TEncoding.UTF8.GetString(Payload));
      end,
      atLeastOnce);

    Sleep(500); // let SUBACK arrive

    Client.Publish('demo/logging/hello', 'world', atLeastOnce);
    Client.Publish('demo/logging/qos2', 'exactly-once', exactlyOnce);

    Sleep(2000);

    Client.Disconnect;
    Writeln;
    Writeln('Total packets observed via events: ', PacketCounter);
    Writeln('Press Enter to exit.');
    Readln;

  except
    on E: Exception do
      Writeln('Error: ', E.Message);
  end;
end.
