unit MQTT.Logger;

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs;

type
  TMQTTLogLevel = (llDebug, llInfo, llWarning, llError, llNone);

  IMQTTLogger = interface
    ['{1F7B2C5D-9A4E-4F8A-B3D1-2E6F8C9A0B1D}']
    procedure Debug(const Msg: string); overload;
    procedure Debug(const Fmt: string; const Args: array of const); overload;
    procedure Info(const Msg: string); overload;
    procedure Info(const Fmt: string; const Args: array of const); overload;
    procedure Warning(const Msg: string); overload;
    procedure Warning(const Fmt: string; const Args: array of const); overload;
    procedure Error(const Msg: string); overload;
    procedure Error(const Fmt: string; const Args: array of const); overload;
    function GetMinLevel: TMQTTLogLevel;
    procedure SetMinLevel(Level: TMQTTLogLevel);
    property MinLevel: TMQTTLogLevel read GetMinLevel write SetMinLevel;
  end;

  TMQTTLoggerBase = class abstract(TInterfacedObject, IMQTTLogger)
  private
    FMinLevel: TMQTTLogLevel;
  protected
    procedure WriteLog(Level: TMQTTLogLevel; const Msg: string); virtual; abstract;
    function FormatLine(Level: TMQTTLogLevel; const Msg: string): string;
    function LevelLabel(Level: TMQTTLogLevel): string;
  public
    constructor Create(AMinLevel: TMQTTLogLevel = llInfo);
    procedure Debug(const Msg: string); overload;
    procedure Debug(const Fmt: string; const Args: array of const); overload;
    procedure Info(const Msg: string); overload;
    procedure Info(const Fmt: string; const Args: array of const); overload;
    procedure Warning(const Msg: string); overload;
    procedure Warning(const Fmt: string; const Args: array of const); overload;
    procedure Error(const Msg: string); overload;
    procedure Error(const Fmt: string; const Args: array of const); overload;
    function GetMinLevel: TMQTTLogLevel;
    procedure SetMinLevel(Level: TMQTTLogLevel);
  end;

  TMQTTNullLogger = class(TMQTTLoggerBase)
  protected
    procedure WriteLog(Level: TMQTTLogLevel; const Msg: string); override;
  end;

  TMQTTConsoleLogger = class(TMQTTLoggerBase)
  private
    FLock: TCriticalSection;
  protected
    procedure WriteLog(Level: TMQTTLogLevel; const Msg: string); override;
  public
    constructor Create(AMinLevel: TMQTTLogLevel = llInfo);
    destructor Destroy; override;
  end;

  TMQTTFileLogger = class(TMQTTLoggerBase)
  private
    FFileName: string;
    FStream: TFileStream;
    FLock: TCriticalSection;
    FAppend: Boolean;
    procedure OpenStream;
  protected
    procedure WriteLog(Level: TMQTTLogLevel; const Msg: string); override;
  public
    constructor Create(const AFileName: string; AAppend: Boolean = True;
      AMinLevel: TMQTTLogLevel = llInfo);
    destructor Destroy; override;
    property FileName: string read FFileName;
  end;

function CreateConsoleLogger(MinLevel: TMQTTLogLevel = llInfo): IMQTTLogger;
function CreateFileLogger(const FileName: string; Append: Boolean = True;
  MinLevel: TMQTTLogLevel = llInfo): IMQTTLogger;
function CreateNullLogger: IMQTTLogger;

implementation

uses
  System.DateUtils;

function CreateConsoleLogger(MinLevel: TMQTTLogLevel): IMQTTLogger;
begin
  Result := TMQTTConsoleLogger.Create(MinLevel);
end;

function CreateFileLogger(const FileName: string; Append: Boolean;
  MinLevel: TMQTTLogLevel): IMQTTLogger;
begin
  Result := TMQTTFileLogger.Create(FileName, Append, MinLevel);
end;

function CreateNullLogger: IMQTTLogger;
begin
  Result := TMQTTNullLogger.Create(llNone);
end;

{ TMQTTLoggerBase }

constructor TMQTTLoggerBase.Create(AMinLevel: TMQTTLogLevel);
begin
  inherited Create;
  FMinLevel := AMinLevel;
end;

function TMQTTLoggerBase.LevelLabel(Level: TMQTTLogLevel): string;
begin
  case Level of
    llDebug:   Result := 'DEBUG';
    llInfo:    Result := 'INFO ';
    llWarning: Result := 'WARN ';
    llError:   Result := 'ERROR';
  else
    Result := '?????';
  end;
end;

function TMQTTLoggerBase.FormatLine(Level: TMQTTLogLevel; const Msg: string): string;
begin
  Result := Format('[%s] [%s] %s',
    [FormatDateTime('yyyy-mm-dd hh:nn:ss.zzz', Now), LevelLabel(Level), Msg]);
end;

function TMQTTLoggerBase.GetMinLevel: TMQTTLogLevel;
begin
  Result := FMinLevel;
end;

procedure TMQTTLoggerBase.SetMinLevel(Level: TMQTTLogLevel);
begin
  FMinLevel := Level;
end;

procedure TMQTTLoggerBase.Debug(const Msg: string);
begin
  if FMinLevel <= llDebug then
    WriteLog(llDebug, Msg);
end;

procedure TMQTTLoggerBase.Debug(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llDebug then
    WriteLog(llDebug, Format(Fmt, Args));
end;

procedure TMQTTLoggerBase.Info(const Msg: string);
begin
  if FMinLevel <= llInfo then
    WriteLog(llInfo, Msg);
end;

procedure TMQTTLoggerBase.Info(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llInfo then
    WriteLog(llInfo, Format(Fmt, Args));
end;

procedure TMQTTLoggerBase.Warning(const Msg: string);
begin
  if FMinLevel <= llWarning then
    WriteLog(llWarning, Msg);
end;

procedure TMQTTLoggerBase.Warning(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llWarning then
    WriteLog(llWarning, Format(Fmt, Args));
end;

procedure TMQTTLoggerBase.Error(const Msg: string);
begin
  if FMinLevel <= llError then
    WriteLog(llError, Msg);
end;

procedure TMQTTLoggerBase.Error(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llError then
    WriteLog(llError, Format(Fmt, Args));
end;

{ TMQTTNullLogger }

procedure TMQTTNullLogger.WriteLog(Level: TMQTTLogLevel; const Msg: string);
begin
  // no-op
end;

{ TMQTTConsoleLogger }

constructor TMQTTConsoleLogger.Create(AMinLevel: TMQTTLogLevel);
begin
  inherited Create(AMinLevel);
  FLock := TCriticalSection.Create;
end;

destructor TMQTTConsoleLogger.Destroy;
begin
  FLock.Free;
  inherited;
end;

procedure TMQTTConsoleLogger.WriteLog(Level: TMQTTLogLevel; const Msg: string);
var
  Line: string;
begin
  Line := FormatLine(Level, Msg);
  FLock.Enter;
  try
    if IsConsole then
      Writeln(Line);
  finally
    FLock.Leave;
  end;
end;

{ TMQTTFileLogger }

constructor TMQTTFileLogger.Create(const AFileName: string; AAppend: Boolean;
  AMinLevel: TMQTTLogLevel);
begin
  inherited Create(AMinLevel);
  FFileName := AFileName;
  FAppend := AAppend;
  FLock := TCriticalSection.Create;
  OpenStream;
end;

destructor TMQTTFileLogger.Destroy;
begin
  FreeAndNil(FStream);
  FLock.Free;
  inherited;
end;

procedure TMQTTFileLogger.OpenStream;
var
  Mode: Word;
begin
  if FAppend and FileExists(FFileName) then
  begin
    FStream := TFileStream.Create(FFileName, fmOpenWrite or fmShareDenyWrite);
    FStream.Seek(0, soEnd);
  end
  else
  begin
    Mode := fmCreate or fmShareDenyWrite;
    FStream := TFileStream.Create(FFileName, Mode);
  end;
end;

procedure TMQTTFileLogger.WriteLog(Level: TMQTTLogLevel; const Msg: string);
var
  Line: string;
  Bytes: TBytes;
begin
  Line := FormatLine(Level, Msg) + sLineBreak;
  Bytes := TEncoding.UTF8.GetBytes(Line);

  FLock.Enter;
  try
    if Assigned(FStream) then
    begin
      FStream.WriteBuffer(Bytes, Length(Bytes));
    end;
  finally
    FLock.Leave;
  end;
end;

end.
