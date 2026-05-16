unit MQTT.Logger;

(*
  Minimal logging contract for the Delphi MQTT client.

  The library core depends only on IMQTTLogger so it stays free of any
  external logging framework. Two trivial implementations ship in this unit:

    * TMQTTNullLogger - the default, swallows everything (zero overhead).
    * TMQTTProcLogger - forwards each call to a user-supplied procedure.
                        Use it from sample code to demonstrate the interface
                        without pulling in a real logger.

  For real applications, plug a production-grade logger via an adapter.
  A ready-made adapter for LoggerPro (https://github.com/danieleteti/loggerpro)
  is provided in the separate unit MQTT.Logger.LoggerPro.pas.

  Why no built-in Console / File logger?
  Real-world Delphi codebases already have a logging library (LoggerPro,
  CodeSiteLogging, log4d, ...). Rolling our own would be a synchronous
  reinvention that competes with the user's existing infrastructure. The
  IMQTTLogger interface is intentionally tiny so any backend can adapt to it
  in a few lines of code.
*)

interface

uses
  System.SysUtils;

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

  TMQTTLogProc = reference to procedure(Level: TMQTTLogLevel; const Msg: string);

  // No-op implementation. Default when no logger is set on the client.
  TMQTTNullLogger = class(TInterfacedObject, IMQTTLogger)
  private
    FMinLevel: TMQTTLogLevel;
  public
    constructor Create;
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

  // Trivial inline adapter: every call (filtered by MinLevel) is forwarded
  // to a user-supplied procedure. Handy for samples and tests.
  TMQTTProcLogger = class(TInterfacedObject, IMQTTLogger)
  private
    FProc: TMQTTLogProc;
    FMinLevel: TMQTTLogLevel;
    procedure Emit(Level: TMQTTLogLevel; const Msg: string);
  public
    constructor Create(const Proc: TMQTTLogProc; MinLevel: TMQTTLogLevel = llInfo);
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

function CreateNullLogger: IMQTTLogger;
function CreateProcLogger(const Proc: TMQTTLogProc;
  MinLevel: TMQTTLogLevel = llInfo): IMQTTLogger;
function LogLevelName(Level: TMQTTLogLevel): string;

implementation

function CreateNullLogger: IMQTTLogger;
begin
  Result := TMQTTNullLogger.Create;
end;

function CreateProcLogger(const Proc: TMQTTLogProc; MinLevel: TMQTTLogLevel): IMQTTLogger;
begin
  Result := TMQTTProcLogger.Create(Proc, MinLevel);
end;

function LogLevelName(Level: TMQTTLogLevel): string;
begin
  case Level of
    llDebug:   Result := 'DEBUG';
    llInfo:    Result := 'INFO';
    llWarning: Result := 'WARN';
    llError:   Result := 'ERROR';
    llNone:    Result := 'NONE';
  else
    Result := '?';
  end;
end;

{ TMQTTNullLogger }

constructor TMQTTNullLogger.Create;
begin
  inherited;
  FMinLevel := llNone;
end;

procedure TMQTTNullLogger.Debug(const Msg: string); begin end;
procedure TMQTTNullLogger.Debug(const Fmt: string; const Args: array of const); begin end;
procedure TMQTTNullLogger.Info(const Msg: string); begin end;
procedure TMQTTNullLogger.Info(const Fmt: string; const Args: array of const); begin end;
procedure TMQTTNullLogger.Warning(const Msg: string); begin end;
procedure TMQTTNullLogger.Warning(const Fmt: string; const Args: array of const); begin end;
procedure TMQTTNullLogger.Error(const Msg: string); begin end;
procedure TMQTTNullLogger.Error(const Fmt: string; const Args: array of const); begin end;

function TMQTTNullLogger.GetMinLevel: TMQTTLogLevel;
begin
  Result := FMinLevel;
end;

procedure TMQTTNullLogger.SetMinLevel(Level: TMQTTLogLevel);
begin
  FMinLevel := Level;
end;

{ TMQTTProcLogger }

constructor TMQTTProcLogger.Create(const Proc: TMQTTLogProc; MinLevel: TMQTTLogLevel);
begin
  inherited Create;
  FProc := Proc;
  FMinLevel := MinLevel;
end;

procedure TMQTTProcLogger.Emit(Level: TMQTTLogLevel; const Msg: string);
begin
  if (FMinLevel <= Level) and Assigned(FProc) then
  begin
    try
      FProc(Level, Msg);
    except
      // never propagate logger exceptions back to the MQTT client
    end;
  end;
end;

procedure TMQTTProcLogger.Debug(const Msg: string);
begin
  Emit(llDebug, Msg);
end;

procedure TMQTTProcLogger.Debug(const Fmt: string; const Args: array of const);
begin
  Emit(llDebug, Format(Fmt, Args));
end;

procedure TMQTTProcLogger.Info(const Msg: string);
begin
  Emit(llInfo, Msg);
end;

procedure TMQTTProcLogger.Info(const Fmt: string; const Args: array of const);
begin
  Emit(llInfo, Format(Fmt, Args));
end;

procedure TMQTTProcLogger.Warning(const Msg: string);
begin
  Emit(llWarning, Msg);
end;

procedure TMQTTProcLogger.Warning(const Fmt: string; const Args: array of const);
begin
  Emit(llWarning, Format(Fmt, Args));
end;

procedure TMQTTProcLogger.Error(const Msg: string);
begin
  Emit(llError, Msg);
end;

procedure TMQTTProcLogger.Error(const Fmt: string; const Args: array of const);
begin
  Emit(llError, Format(Fmt, Args));
end;

function TMQTTProcLogger.GetMinLevel: TMQTTLogLevel;
begin
  Result := FMinLevel;
end;

procedure TMQTTProcLogger.SetMinLevel(Level: TMQTTLogLevel);
begin
  FMinLevel := Level;
end;

end.
