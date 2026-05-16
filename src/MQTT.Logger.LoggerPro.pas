unit MQTT.Logger.LoggerPro;

(*
  Adapter that lets the Delphi MQTT client log through a LoggerPro ILogWriter
  (https://github.com/danieleteti/loggerpro).

  This unit is OPTIONAL. The core library (MQTT.Client + MQTT.Logger) does
  not link against LoggerPro. Add this unit to your project only when you
  want LoggerPro to receive the client's log messages.

  Mapping
  -------
    IMQTTLogger.Debug   ->  ILogWriter.Debug(Msg, Tag)
    IMQTTLogger.Info    ->  ILogWriter.Info (Msg, Tag)
    IMQTTLogger.Warning ->  ILogWriter.Warn (Msg, Tag)        (note: name change)
    IMQTTLogger.Error   ->  ILogWriter.Error(Msg, Tag)

  Level filter
  ------------
    IMQTTLogger applies its own MinLevel filter BEFORE handing off to
    LoggerPro. LoggerPro applies its own per-appender filtering on top of
    that. Set IMQTTLogger.MinLevel to llDebug if you want LoggerPro to see
    every message and decide on its own.

  Usage
  -----
    uses LoggerPro, LoggerPro.ConsoleAppender, LoggerPro.FileAppender,
         MQTT.Logger, MQTT.Logger.LoggerPro, MQTT.Client;

    var
      Writer: ILogWriter;
      Client: IMQTTClient;
    begin
      Writer := BuildLogWriter([
        TLoggerProConsoleAppender.Create,
        TLoggerProFileAppender.Create(10, 5)
      ]);

      Client := CreateMQTTClient;
      Client.Logger := WrapLoggerPro(Writer, 'MQTT', llDebug);
      Client.Connect('localhost', 1883);
    end;
*)

interface

uses
  System.SysUtils,
  LoggerPro,
  MQTT.Logger;

type
  TMQTTLoggerProAdapter = class(TInterfacedObject, IMQTTLogger)
  private
    FWriter: ILogWriter;
    FTag: string;
    FMinLevel: TMQTTLogLevel;
  public
    constructor Create(const Writer: ILogWriter; const Tag: string = 'MQTT';
      MinLevel: TMQTTLogLevel = llInfo);

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

    property Writer: ILogWriter read FWriter;
    property Tag: string read FTag;
  end;

function WrapLoggerPro(const Writer: ILogWriter; const Tag: string = 'MQTT';
  MinLevel: TMQTTLogLevel = llInfo): IMQTTLogger;

implementation

function WrapLoggerPro(const Writer: ILogWriter; const Tag: string;
  MinLevel: TMQTTLogLevel): IMQTTLogger;
begin
  Result := TMQTTLoggerProAdapter.Create(Writer, Tag, MinLevel);
end;

{ TMQTTLoggerProAdapter }

constructor TMQTTLoggerProAdapter.Create(const Writer: ILogWriter;
  const Tag: string; MinLevel: TMQTTLogLevel);
begin
  inherited Create;
  if Writer = nil then
    raise EArgumentNilException.Create('TMQTTLoggerProAdapter requires a non-nil ILogWriter');
  FWriter := Writer;
  FTag := Tag;
  FMinLevel := MinLevel;
end;

procedure TMQTTLoggerProAdapter.Debug(const Msg: string);
begin
  if FMinLevel <= llDebug then
    FWriter.Debug(Msg, FTag);
end;

procedure TMQTTLoggerProAdapter.Debug(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llDebug then
    FWriter.Debug(Format(Fmt, Args), FTag);
end;

procedure TMQTTLoggerProAdapter.Info(const Msg: string);
begin
  if FMinLevel <= llInfo then
    FWriter.Info(Msg, FTag);
end;

procedure TMQTTLoggerProAdapter.Info(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llInfo then
    FWriter.Info(Format(Fmt, Args), FTag);
end;

procedure TMQTTLoggerProAdapter.Warning(const Msg: string);
begin
  if FMinLevel <= llWarning then
    FWriter.Warn(Msg, FTag);
end;

procedure TMQTTLoggerProAdapter.Warning(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llWarning then
    FWriter.Warn(Format(Fmt, Args), FTag);
end;

procedure TMQTTLoggerProAdapter.Error(const Msg: string);
begin
  if FMinLevel <= llError then
    FWriter.Error(Msg, FTag);
end;

procedure TMQTTLoggerProAdapter.Error(const Fmt: string; const Args: array of const);
begin
  if FMinLevel <= llError then
    FWriter.Error(Format(Fmt, Args), FTag);
end;

function TMQTTLoggerProAdapter.GetMinLevel: TMQTTLogLevel;
begin
  Result := FMinLevel;
end;

procedure TMQTTLoggerProAdapter.SetMinLevel(Level: TMQTTLogLevel);
begin
  FMinLevel := Level;
end;

end.
