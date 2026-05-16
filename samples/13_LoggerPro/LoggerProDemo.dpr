program LoggerProDemo;

{$APPTYPE CONSOLE}

(*
  Plugging LoggerPro into the Delphi MQTT client.

  This sample wires:
    - TLoggerProConsoleAppender  -> stdout, async, color-coded
    - TLoggerProFileAppender     -> logs/LoggerProDemo.*.log, async, rotated

  ...behind the MQTT client's IMQTTLogger via TMQTTLoggerProAdapter.

  Requirements:
    - LoggerPro at C:\DEV\LoggerPro\ (or whatever path you pass to -U)
    - MQTT broker on localhost:1883
*)

uses
  System.SysUtils,
  System.IOUtils,
  LoggerPro,
  LoggerPro.ConsoleAppender,
  LoggerPro.FileAppender,
  MQTT.Types in '..\..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\..\src\MQTT.Logger.pas',
  MQTT.Logger.LoggerPro in '..\..\src\MQTT.Logger.LoggerPro.pas',
  MQTT.Client in '..\..\src\MQTT.Client.pas';

var
  Client: IMQTTClient;
  Options: TMQTTConnectOptions;
  Log: ILogWriter;

begin
  try
    Writeln('========================================');
    Writeln('  Delphi MQTT - LoggerPro integration');
    Writeln('========================================');
    Writeln;

    // 1. Build a LoggerPro writer with two async appenders
    Log := BuildLogWriter([
      TLoggerProConsoleAppender.Create,
      TLoggerProFileAppender.Create(5, 1000, TPath.Combine(GetCurrentDir, 'logs'))
    ]);

    // 2. Wrap it as IMQTTLogger and attach to the client
    Client := CreateMQTTClient;
    Client.Logger := WrapLoggerPro(Log, 'MQTT', llDebug);

    // 3. Use the client as usual; everything is logged via LoggerPro
    Options.SetDefaults;
    Options.ClientID := 'LoggerProDemo';
    Options.Version := MQTT311;

    Client.Connect('localhost', 1883, Options);

    Client.Subscribe('demo/loggerpro/echo',
      procedure(const Topic: string; const Payload: TBytes)
      begin
        Log.Info(Format('app received "%s" on %s',
          [TEncoding.UTF8.GetString(Payload), Topic]), 'APP');
      end,
      atLeastOnce);

    Sleep(400); // let SUBACK settle

    Log.Info('publishing demo messages', 'APP');
    Client.Publish('demo/loggerpro/echo', 'hello-loggerpro', atLeastOnce);
    Client.Publish('demo/loggerpro/echo', 'second-message', exactlyOnce);

    Sleep(1500);

    Client.Disconnect;
    Writeln;
    Writeln('Done. Check the logs/ folder for the rotated log file.');
    Writeln('Press Enter to exit.');
    Readln;

  except
    on E: Exception do
      Writeln('Error: ', E.ClassName, ': ', E.Message);
  end;
end.
