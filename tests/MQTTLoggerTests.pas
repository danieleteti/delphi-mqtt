unit MQTTLoggerTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.IOUtils,
  MQTT.Logger;

type
  [TestFixture]
  TMQTTLoggerTests = class
  private
    FTempFile: string;
  public
    [Setup]
    procedure Setup;
    [TearDown]
    procedure TearDown;

    [Test]
    procedure NullLogger_DoesNotRaise;
    [Test]
    procedure ConsoleLogger_LevelFilter_Suppresses;
    [Test]
    procedure ConsoleLogger_LevelFilter_Allows;
    [Test]
    procedure FileLogger_WritesLine;
    [Test]
    procedure FileLogger_MultipleLines;
    [Test]
    procedure FileLogger_AppendMode_PreservesPrevious;
    [Test]
    procedure FileLogger_TruncateMode_Overwrites;
    [Test]
    procedure FileLogger_FormatArgs;
    [Test]
    procedure LoggerLevel_NoneSuppressesAll;
  end;

implementation

procedure TMQTTLoggerTests.Setup;
begin
  FTempFile := TPath.Combine(TPath.GetTempPath,
    'mqtt-log-test-' + FormatDateTime('yyyymmddhhnnsszzz', Now) + '.log');
end;

procedure TMQTTLoggerTests.TearDown;
begin
  if (FTempFile <> '') and TFile.Exists(FTempFile) then
    TFile.Delete(FTempFile);
end;

procedure TMQTTLoggerTests.NullLogger_DoesNotRaise;
var
  L: IMQTTLogger;
begin
  L := CreateNullLogger;
  L.Debug('debug');
  L.Info('info');
  L.Warning('warn');
  L.Error('err');
  Assert.Pass('no exception');
end;

procedure TMQTTLoggerTests.ConsoleLogger_LevelFilter_Suppresses;
var
  L: IMQTTLogger;
begin
  L := CreateConsoleLogger(llError);
  Assert.AreEqual(Ord(llError), Ord(L.MinLevel));
  L.Debug('should not appear');
  L.Info('should not appear');
  Assert.Pass('level filter ok');
end;

procedure TMQTTLoggerTests.ConsoleLogger_LevelFilter_Allows;
var
  L: IMQTTLogger;
begin
  L := CreateConsoleLogger(llDebug);
  L.SetMinLevel(llInfo);
  Assert.AreEqual(Ord(llInfo), Ord(L.MinLevel));
end;

procedure TMQTTLoggerTests.FileLogger_WritesLine;
var
  L: IMQTTLogger;
  Content: string;
begin
  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('hello file');
  L := nil; // close stream
  Assert.IsTrue(TFile.Exists(FTempFile));
  Content := TFile.ReadAllText(FTempFile, TEncoding.UTF8);
  Assert.IsTrue(Content.Contains('hello file'));
  Assert.IsTrue(Content.Contains('INFO'));
end;

procedure TMQTTLoggerTests.FileLogger_MultipleLines;
var
  L: IMQTTLogger;
  Lines: TArray<string>;
begin
  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('line1');
  L.Warning('line2');
  L.Error('line3');
  L := nil;
  Lines := TFile.ReadAllLines(FTempFile, TEncoding.UTF8);
  Assert.AreEqual<NativeInt>(3, Length(Lines));
end;

procedure TMQTTLoggerTests.FileLogger_AppendMode_PreservesPrevious;
var
  L: IMQTTLogger;
  Lines: TArray<string>;
begin
  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('first');
  L := nil;

  L := CreateFileLogger(FTempFile, True, llDebug);
  L.Info('second');
  L := nil;

  Lines := TFile.ReadAllLines(FTempFile, TEncoding.UTF8);
  Assert.AreEqual<NativeInt>(2, Length(Lines));
  Assert.IsTrue(Lines[0].Contains('first'));
  Assert.IsTrue(Lines[1].Contains('second'));
end;

procedure TMQTTLoggerTests.FileLogger_TruncateMode_Overwrites;
var
  L: IMQTTLogger;
  Lines: TArray<string>;
begin
  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('first');
  L := nil;

  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('second');
  L := nil;

  Lines := TFile.ReadAllLines(FTempFile, TEncoding.UTF8);
  Assert.AreEqual<NativeInt>(1, Length(Lines));
  Assert.IsTrue(Lines[0].Contains('second'));
end;

procedure TMQTTLoggerTests.FileLogger_FormatArgs;
var
  L: IMQTTLogger;
  Content: string;
begin
  L := CreateFileLogger(FTempFile, False, llDebug);
  L.Info('value=%d name=%s', [42, 'foo']);
  L := nil;
  Content := TFile.ReadAllText(FTempFile, TEncoding.UTF8);
  Assert.IsTrue(Content.Contains('value=42 name=foo'));
end;

procedure TMQTTLoggerTests.LoggerLevel_NoneSuppressesAll;
var
  L: IMQTTLogger;
  Content: string;
begin
  L := CreateFileLogger(FTempFile, False, llNone);
  L.Debug('debug');
  L.Info('info');
  L.Warning('warn');
  L.Error('err');
  L := nil;
  if TFile.Exists(FTempFile) then
  begin
    Content := TFile.ReadAllText(FTempFile, TEncoding.UTF8);
    Assert.AreEqual('', Content.Trim);
  end;
end;

initialization
  TDUnitX.RegisterTestFixture(TMQTTLoggerTests);

end.
