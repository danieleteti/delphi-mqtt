program MqttTests;

{$APPTYPE CONSOLE}
{$STRONGLINKTYPES ON}

uses
  System.SysUtils,
  {$IFDEF TESTINSIGHT}
  TestInsight.DUnitX,
  {$ENDIF }
  DUnitX.Loggers.Console,
  DUnitX.Loggers.Xml.NUnit,
  DUnitX.TestFramework,
  MQTT.Types in '..\src\MQTT.Types.pas',
  MQTT.Protocol in '..\src\MQTT.Protocol.pas',
  MQTT.Logger in '..\src\MQTT.Logger.pas',
  MQTT.Client in '..\src\MQTT.Client.pas',
  MQTTProtocolTests in 'MQTTProtocolTests.pas',
  MQTTLoggerTests in 'MQTTLoggerTests.pas',
  MQTTClientTests in 'MQTTClientTests.pas',
  MQTTPublicBrokerTests in 'MQTTPublicBrokerTests.pas';

var
  Runner: ITestRunner;
  Results: IRunResults;
  Logger: ITestLogger;
  NUnitLogger: ITestLogger;
  ExitCode: Integer;
begin
  try
    TDUnitX.CheckCommandLine;
    Runner := TDUnitX.CreateRunner;
    Runner.UseRTTI := True;
    Runner.FailsOnNoAsserts := False;

    Logger := TDUnitXConsoleLogger.Create(True);
    Runner.AddLogger(Logger);

    NUnitLogger := TDUnitXXMLNUnitFileLogger.Create(TDUnitX.Options.XMLOutputFile);
    Runner.AddLogger(NUnitLogger);

    Results := Runner.Execute;

    if Results.FailureCount > 0 then
      ExitCode := 1
    else if Results.ErrorCount > 0 then
      ExitCode := 2
    else
      ExitCode := 0;

    if TDUnitX.Options.ExitBehavior = TDUnitXExitBehavior.Pause then
    begin
      System.Write('Press <Enter> to exit.');
      System.Readln;
    end;

    System.ExitCode := ExitCode;
  except
    on E: Exception do
    begin
      System.Writeln(E.ClassName, ': ', E.Message);
      System.ExitCode := 3;
    end;
  end;
end.
