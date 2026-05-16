unit MQTTLoggerTests;

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  MQTT.Logger;

type
  [TestFixture]
  TMQTTLoggerTests = class
  public
    [Test]
    procedure NullLogger_DoesNotRaise;
    [Test]
    procedure NullLogger_GetMinLevelDefaultsToNone;

    [Test]
    procedure ProcLogger_ForwardsToCallback;
    [Test]
    procedure ProcLogger_LevelFilter_SuppressesLowerLevels;
    [Test]
    procedure ProcLogger_LevelFilter_AllowsAtOrAboveMinLevel;
    [Test]
    procedure ProcLogger_FormatOverload_FormatsArgs;
    [Test]
    procedure ProcLogger_NoneLevel_SuppressesEverything;
    [Test]
    procedure ProcLogger_DispatchSurvivesCallbackException;
    [Test]
    procedure ProcLogger_NilProcDoesNothing;

    [Test]
    procedure LogLevelName_KnownLevels;
  end;

implementation

procedure TMQTTLoggerTests.NullLogger_DoesNotRaise;
var
  L: IMQTTLogger;
begin
  L := CreateNullLogger;
  L.Debug('debug');
  L.Info('info');
  L.Warning('warn');
  L.Error('err');
  L.Debug('fmt %d', [1]);
  Assert.Pass('no exception');
end;

procedure TMQTTLoggerTests.NullLogger_GetMinLevelDefaultsToNone;
var
  L: IMQTTLogger;
begin
  L := CreateNullLogger;
  Assert.AreEqual(Ord(llNone), Ord(L.MinLevel));
end;

procedure TMQTTLoggerTests.ProcLogger_ForwardsToCallback;
var
  L: IMQTTLogger;
  Calls: TStringList;
begin
  Calls := TStringList.Create;
  try
    L := CreateProcLogger(
      procedure(Level: TMQTTLogLevel; const Msg: string)
      begin
        Calls.Add(Format('%s:%s', [LogLevelName(Level), Msg]));
      end,
      llDebug);
    L.Debug('a');
    L.Info('b');
    L.Warning('c');
    L.Error('d');
    Assert.AreEqual<NativeInt>(4, Calls.Count);
    Assert.AreEqual('DEBUG:a', Calls[0]);
    Assert.AreEqual('INFO:b', Calls[1]);
    Assert.AreEqual('WARN:c', Calls[2]);
    Assert.AreEqual('ERROR:d', Calls[3]);
  finally
    Calls.Free;
  end;
end;

procedure TMQTTLoggerTests.ProcLogger_LevelFilter_SuppressesLowerLevels;
var
  L: IMQTTLogger;
  Calls: TStringList;
begin
  Calls := TStringList.Create;
  try
    L := CreateProcLogger(
      procedure(Level: TMQTTLogLevel; const Msg: string)
      begin
        Calls.Add(Msg);
      end,
      llWarning);
    L.Debug('debug-suppressed');
    L.Info('info-suppressed');
    L.Warning('warn-shown');
    L.Error('error-shown');
    Assert.AreEqual<NativeInt>(2, Calls.Count);
    Assert.AreEqual('warn-shown', Calls[0]);
    Assert.AreEqual('error-shown', Calls[1]);
  finally
    Calls.Free;
  end;
end;

procedure TMQTTLoggerTests.ProcLogger_LevelFilter_AllowsAtOrAboveMinLevel;
var
  L: IMQTTLogger;
  Hit: Boolean;
begin
  Hit := False;
  L := CreateProcLogger(
    procedure(Level: TMQTTLogLevel; const Msg: string)
    begin
      Hit := True;
    end,
    llInfo);
  L.SetMinLevel(llError);
  Assert.AreEqual(Ord(llError), Ord(L.MinLevel));
  L.Warning('not delivered');
  Assert.IsFalse(Hit);
  L.Error('delivered');
  Assert.IsTrue(Hit);
end;

procedure TMQTTLoggerTests.ProcLogger_FormatOverload_FormatsArgs;
var
  L: IMQTTLogger;
  Last: string;
begin
  Last := '';
  L := CreateProcLogger(
    procedure(Level: TMQTTLogLevel; const Msg: string)
    begin
      Last := Msg;
    end,
    llDebug);
  L.Info('value=%d name=%s', [42, 'foo']);
  Assert.AreEqual('value=42 name=foo', Last);
end;

procedure TMQTTLoggerTests.ProcLogger_NoneLevel_SuppressesEverything;
var
  L: IMQTTLogger;
  Count: Integer;
begin
  Count := 0;
  L := CreateProcLogger(
    procedure(Level: TMQTTLogLevel; const Msg: string)
    begin
      Inc(Count);
    end,
    llNone);
  L.Debug('a'); L.Info('b'); L.Warning('c'); L.Error('d');
  Assert.AreEqual<NativeInt>(0, Count);
end;

procedure TMQTTLoggerTests.ProcLogger_DispatchSurvivesCallbackException;
var
  L: IMQTTLogger;
begin
  L := CreateProcLogger(
    procedure(Level: TMQTTLogLevel; const Msg: string)
    begin
      raise Exception.Create('callback boom');
    end,
    llDebug);
  // ProcLogger.Emit must swallow callback exceptions so the MQTT client
  // is never destabilised by a misbehaving logger.
  L.Info('should not propagate');
  Assert.Pass('exception swallowed');
end;

procedure TMQTTLoggerTests.ProcLogger_NilProcDoesNothing;
var
  L: IMQTTLogger;
begin
  L := CreateProcLogger(nil, llDebug);
  L.Info('no-op');
  L.Error('also no-op');
  Assert.Pass('no AV');
end;

procedure TMQTTLoggerTests.LogLevelName_KnownLevels;
begin
  Assert.AreEqual('DEBUG', LogLevelName(llDebug));
  Assert.AreEqual('INFO',  LogLevelName(llInfo));
  Assert.AreEqual('WARN',  LogLevelName(llWarning));
  Assert.AreEqual('ERROR', LogLevelName(llError));
  Assert.AreEqual('NONE',  LogLevelName(llNone));
end;

initialization
  TDUnitX.RegisterTestFixture(TMQTTLoggerTests);

end.
