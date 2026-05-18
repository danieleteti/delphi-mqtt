unit MQTT.Client;

interface

uses
  System.SysUtils,
  System.Classes,
  System.Threading,
  System.SyncObjs,
  System.Generics.Collections,
  System.DateUtils,
  System.Math,
  IdTCPClient,
  IdGlobal,
  IdException,
  IdExceptionCore,
  IdSSLOpenSSL,
  IdSSLOpenSSLHeaders,
  MQTT.Types,
  MQTT.Protocol,
  MQTT.Logger;

type
  TMQTTHandler = reference to procedure(const Topic: string; const Payload: TBytes);

  TPendingPublish = record
    PacketID: Word;
    Topic: string;
    Payload: TBytes;
    QoS: TMQTTQoS;
    Retain: Boolean;
    Timestamp: TDateTime;
    RetryCount: Integer;
    State: (psPending, psWaitingPubRec, psWaitingPubComp);
    // Per-publish wakeup signal. Non-nil only for PublishSync callers; the
    // ACK handlers (PubAck/PubComp) signal this event when the matching
    // PacketID is removed from FPendingPublish. NEVER share between calls.
    AckEvent: TEvent;
  end;

  TPendingSubscription = record
    Filter: string;
    QoS: TMQTTQoS;
    Handler: TMQTTHandler;
  end;

  TSubscriptionInfo = record
    Handler: TMQTTHandler;
    ExtendedHandler: TMQTTExtendedHandler;
    ManualAckHandler: TMQTTManualAckHandler;
    HandlerType: (htSimple, htExtended, htManualAck);
    QoS: TMQTTQoS;
    // True when this entry was created via RestoreSubscriptionHandler:
    // the local dispatcher is registered but the subscription exists only
    // broker-side (session resumed with SessionPresent=True). The QoS field
    // above is not meaningful in this case. ResubscribeAll skips these
    // entries on auto-reconnect because the original QoS is unknown.
    Bound: Boolean;
  end;

  TPendingQoS2Inbound = record
    Topic: string;
    Payload: TBytes;
    Dup: Boolean;
  end;

  IMQTTClient = interface
    ['{6E69B8A7-88D4-4B3A-B6B1-D6502E7A0F7E}']
    procedure Connect(const Host: string; Port: Word = 1883); overload;
    procedure Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions); overload;
    procedure Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions; const SSLOptions: TMQTTSSLOptions); overload;
    procedure ConnectSSL(const Host: string; Port: Word = 8883); overload;
    procedure ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions); overload;
    procedure ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions; const SSLOptions: TMQTTSSLOptions); overload;
    procedure Disconnect;
    procedure Publish(const Topic: string; const Payload: string; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False); overload;
    procedure Publish(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False); overload;
    function PublishSync(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS; Retain: Boolean; TimeoutMs: Cardinal = 5000): Boolean;
    procedure Subscribe(const Topic: string; Handler: TMQTTHandler; QoS: TMQTTQoS = atMostOnce); overload;
    procedure Subscribe(const Topic: string; Handler: TMQTTMessageEvent; QoS: TMQTTQoS = atMostOnce); overload;
    procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckHandler; QoS: TMQTTQoS = atLeastOnce); overload;
    procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckEvent; QoS: TMQTTQoS = atLeastOnce); overload;
    procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedHandler; QoS: TMQTTQoS = atMostOnce); overload;
    procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedEvent; QoS: TMQTTQoS = atMostOnce); overload;
    // Re-attach the local dispatcher to a subscription that already exists
    // broker-side (typical usage after Connect when LastSessionPresent=True).
    // Does NOT send SUBSCRIBE on the wire. The effective QoS of received
    // PUBLISH packets is the one stored by the broker for the original
    // subscription, not a parameter of this call.
    procedure RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTHandler); overload;
    procedure RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTMessageEvent); overload;
    procedure Unsubscribe(const Topic: string);
    function IsConnected: Boolean;
    function GetState: TMQTTConnectionState;
    procedure SetAutoReconnect(Value: Boolean);
    function GetAutoReconnect: Boolean;
    procedure SetOnDisconnect(Handler: TMQTTDisconnectHandler); overload;
    procedure SetOnDisconnect(Handler: TMQTTDisconnectEvent); overload;
    procedure SetOnError(Handler: TMQTTErrorHandler); overload;
    procedure SetOnError(Handler: TMQTTErrorEvent); overload;
    procedure SetOnConnect(Handler: TMQTTConnectHandler); overload;
    procedure SetOnConnect(Handler: TMQTTConnectEvent); overload;
    procedure SetOnPacketReceived(Handler: TMQTTPacketHandler); overload;
    procedure SetOnPacketReceived(Handler: TMQTTPacketEvent); overload;
    procedure SetOnPacketSent(Handler: TMQTTPacketHandler); overload;
    procedure SetOnPacketSent(Handler: TMQTTPacketEvent); overload;
    procedure SetOnSubscribeAck(Handler: TMQTTSubscribeAckHandler); overload;
    procedure SetOnSubscribeAck(Handler: TMQTTSubscribeAckEvent); overload;
    procedure SetLogger(const Logger: IMQTTLogger);
    function GetLogger: IMQTTLogger;
    function GetLastSessionPresent: Boolean;
    property Connected: Boolean read IsConnected;
    property State: TMQTTConnectionState read GetState;
    property AutoReconnect: Boolean read GetAutoReconnect write SetAutoReconnect;
    property Logger: IMQTTLogger read GetLogger write SetLogger;
    // True if the last CONNACK reported Session Present = 1
    // (broker had a persistent session for this ClientID).
    property LastSessionPresent: Boolean read GetLastSessionPresent;
  end;

  TMQTTClient = class(TInterfacedObject, IMQTTClient)
  private
    FIndy: TIdTCPClient;
    FSSLHandler: TIdSSLIOHandlerSocketOpenSSL;
    FSSLOptions: TMQTTSSLOptions;
    FState: TMQTTConnectionState;
    FReceiver: ITask;
    FPinger: ITask;
    FSubscriptions: TDictionary<string, TSubscriptionInfo>;
    FPendingPublish: TDictionary<Word, TPendingPublish>;
    FPendingQoS2Inbound: TDictionary<Word, TPendingQoS2Inbound>;
    FLock: TCriticalSection;
    FWriteLock: TCriticalSection;
    FOptions: TMQTTConnectOptions;
    FHost: string;
    FPort: Word;
    FNextPacketID: Word;
    FLastActivity: TDateTime;
    FAutoReconnect: Boolean;
    FReconnectDelayMs: Integer;
    FMaxReconnectDelayMs: Integer;
    FOnDisconnect: TMQTTDisconnectHandler;
    FOnError: TMQTTErrorHandler;
    FOnConnect: TMQTTConnectHandler;
    FOnPacketReceived: TMQTTPacketHandler;
    FOnPacketSent: TMQTTPacketHandler;
    FOnSubscribeAck: TMQTTSubscribeAckHandler;
    FLogger: IMQTTLogger;
    FShutdown: Boolean;
    FLastSessionPresent: Boolean;

    function GetNextPacketID: Word;
    function ReadPacket: TBytes;
    procedure SendPacket(const Packet: TBytes);
    procedure ReceiverLoop;
    procedure PingerLoop;
    procedure HandlePacket(const Packet: TBytes);
    procedure HandlePublish(const Packet: TBytes);
    procedure HandlePubAck(const Packet: TBytes);
    procedure HandlePubRec(const Packet: TBytes);
    procedure HandlePubRel(const Packet: TBytes);
    procedure HandlePubComp(const Packet: TBytes);
    procedure HandleSubAck(const Packet: TBytes);
    procedure HandleUnsubAck(const Packet: TBytes);
    procedure HandlePingResp;
    procedure HandleDisconnect(const Packet: TBytes);
    procedure DispatchMessageEx(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS; PacketID: Word);
    function DispatchMessageManualAck(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS; PacketID: Word): Boolean;
    function HasManualAckSubscription(const Topic: string): Boolean;
    procedure SendAck(QoS: TMQTTQoS; PacketID: Word);
    procedure DoReconnect;
    procedure DoDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
    procedure DoError(const Msg: string);
    procedure RetryPendingPublishes;
    procedure ResubscribeAll;
    function IsConnected: Boolean;
    function GetState: TMQTTConnectionState;
    procedure SetAutoReconnect(Value: Boolean);
    function GetAutoReconnect: Boolean;
    procedure SetOnDisconnect(Handler: TMQTTDisconnectHandler); overload;
    procedure SetOnDisconnect(Handler: TMQTTDisconnectEvent); overload;
    procedure SetOnError(Handler: TMQTTErrorHandler); overload;
    procedure SetOnError(Handler: TMQTTErrorEvent); overload;
    procedure SetOnConnect(Handler: TMQTTConnectHandler); overload;
    procedure SetOnConnect(Handler: TMQTTConnectEvent); overload;
    procedure SetOnPacketReceived(Handler: TMQTTPacketHandler); overload;
    procedure SetOnPacketReceived(Handler: TMQTTPacketEvent); overload;
    procedure SetOnPacketSent(Handler: TMQTTPacketHandler); overload;
    procedure SetOnPacketSent(Handler: TMQTTPacketEvent); overload;
    procedure SetOnSubscribeAck(Handler: TMQTTSubscribeAckHandler); overload;
    procedure SetOnSubscribeAck(Handler: TMQTTSubscribeAckEvent); overload;
    procedure SetLogger(const Logger: IMQTTLogger);
    function GetLogger: IMQTTLogger;
    function GetLastSessionPresent: Boolean;
    function SnapshotLogger: IMQTTLogger;
    procedure NotifyPacketSent(const Packet: TBytes);
    procedure NotifyPacketReceived(const Packet: TBytes);
    function PacketTypeName(PT: TMQTTPacketType): string;
    procedure ConfigureSSL;
    procedure CleanupSSL;
    function GetSSLMethodVersion(Method: TMQTTSSLMethod): TIdSSLVersion;
    procedure SSLGetPassword(var Password: string);
  public
    constructor Create;
    destructor Destroy; override;
    procedure Connect(const Host: string; Port: Word = 1883); overload;
    procedure Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions); overload;
    procedure Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions; const SSLOptions: TMQTTSSLOptions); overload;
    procedure ConnectSSL(const Host: string; Port: Word = 8883); overload;
    procedure ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions); overload;
    procedure ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions; const SSLOptions: TMQTTSSLOptions); overload;
    procedure Disconnect;
    procedure Publish(const Topic: string; const Payload: string; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False); overload;
    procedure Publish(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS = atMostOnce; Retain: Boolean = False); overload;
    function PublishSync(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS; Retain: Boolean; TimeoutMs: Cardinal = 5000): Boolean;
    procedure Subscribe(const Topic: string; Handler: TMQTTHandler; QoS: TMQTTQoS = atMostOnce); overload;
    procedure Subscribe(const Topic: string; Handler: TMQTTMessageEvent; QoS: TMQTTQoS = atMostOnce); overload;
    procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckHandler; QoS: TMQTTQoS = atLeastOnce); overload;
    procedure SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckEvent; QoS: TMQTTQoS = atLeastOnce); overload;
    procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedHandler; QoS: TMQTTQoS = atMostOnce); overload;
    procedure SubscribeEx(const Topic: string; Handler: TMQTTExtendedEvent; QoS: TMQTTQoS = atMostOnce); overload;
    procedure RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTHandler); overload;
    procedure RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTMessageEvent); overload;
    procedure Unsubscribe(const Topic: string);
  end;

function CreateMQTTClient: IMQTTClient;

implementation

function CreateMQTTClient: IMQTTClient;
begin
  Result := TMQTTClient.Create;
end;

{ TMQTTClient }

constructor TMQTTClient.Create;
begin
  inherited Create;
  FIndy := TIdTCPClient.Create(nil);
  FIndy.ConnectTimeout := 5000;
  FIndy.ReadTimeout := 100;
  FSSLHandler := nil;
  FSSLOptions.SetDefaults;
  FSubscriptions := TDictionary<string, TSubscriptionInfo>.Create;
  FPendingPublish := TDictionary<Word, TPendingPublish>.Create;
  FPendingQoS2Inbound := TDictionary<Word, TPendingQoS2Inbound>.Create;
  FLock := TCriticalSection.Create;
  FWriteLock := TCriticalSection.Create;
  FState := Disconnected;
  FNextPacketID := 1;
  FAutoReconnect := False;
  FReconnectDelayMs := 1000;
  FMaxReconnectDelayMs := 30000;
  FShutdown := False;
  FLogger := CreateNullLogger;
  FOptions.SetDefaults;
end;

destructor TMQTTClient.Destroy;
begin
  FShutdown := True;
  Disconnect;
  CleanupSSL;
  FPendingQoS2Inbound.Free;
  FPendingPublish.Free;
  FSubscriptions.Free;
  FWriteLock.Free;
  FLock.Free;
  FIndy.Free;
  inherited Destroy;
end;

function TMQTTClient.GetNextPacketID: Word;
begin
  FLock.Enter;
  try
    Result := FNextPacketID;
    Inc(FNextPacketID);
    if FNextPacketID = 0 then
      FNextPacketID := 1;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.IsConnected: Boolean;
begin
  Result := (FState = Connected) and FIndy.Connected;
end;

function TMQTTClient.GetState: TMQTTConnectionState;
begin
  Result := FState;
end;

procedure TMQTTClient.SetAutoReconnect(Value: Boolean);
begin
  FAutoReconnect := Value;
end;

function TMQTTClient.GetAutoReconnect: Boolean;
begin
  Result := FAutoReconnect;
end;

procedure TMQTTClient.SetOnDisconnect(Handler: TMQTTDisconnectHandler);
begin
  FOnDisconnect := Handler;
end;

procedure TMQTTClient.SetOnDisconnect(Handler: TMQTTDisconnectEvent);
begin
  if Assigned(Handler) then
    FOnDisconnect := procedure(ReasonCode: TMQTTReasonCode; const ReasonString: string)
      begin Handler(ReasonCode, ReasonString); end
  else
    FOnDisconnect := nil;
end;

procedure TMQTTClient.SetOnError(Handler: TMQTTErrorHandler);
begin
  FOnError := Handler;
end;

procedure TMQTTClient.SetOnError(Handler: TMQTTErrorEvent);
begin
  if Assigned(Handler) then
    FOnError := procedure(const ErrorMsg: string) begin Handler(ErrorMsg); end
  else
    FOnError := nil;
end;

procedure TMQTTClient.SetOnConnect(Handler: TMQTTConnectHandler);
begin
  FOnConnect := Handler;
end;

procedure TMQTTClient.SetOnConnect(Handler: TMQTTConnectEvent);
begin
  if Assigned(Handler) then
    FOnConnect := procedure(ReasonCode: TMQTTReasonCode; SessionPresent: Boolean) begin Handler(ReasonCode, SessionPresent); end
  else
    FOnConnect := nil;
end;

procedure TMQTTClient.SetOnPacketReceived(Handler: TMQTTPacketHandler);
begin
  FOnPacketReceived := Handler;
end;

procedure TMQTTClient.SetOnPacketReceived(Handler: TMQTTPacketEvent);
begin
  if Assigned(Handler) then
    FOnPacketReceived := procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin Handler(PT, Raw); end
  else
    FOnPacketReceived := nil;
end;

procedure TMQTTClient.SetOnPacketSent(Handler: TMQTTPacketHandler);
begin
  FOnPacketSent := Handler;
end;

procedure TMQTTClient.SetOnPacketSent(Handler: TMQTTPacketEvent);
begin
  if Assigned(Handler) then
    FOnPacketSent := procedure(PT: TMQTTPacketType; const Raw: TBytes)
      begin Handler(PT, Raw); end
  else
    FOnPacketSent := nil;
end;

procedure TMQTTClient.SetOnSubscribeAck(Handler: TMQTTSubscribeAckHandler);
begin
  FOnSubscribeAck := Handler;
end;

procedure TMQTTClient.SetOnSubscribeAck(Handler: TMQTTSubscribeAckEvent);
begin
  if Assigned(Handler) then
    FOnSubscribeAck := procedure(PacketID: Word; const GrantedQoS: TArray<Byte>)
      begin Handler(PacketID, GrantedQoS); end
  else
    FOnSubscribeAck := nil;
end;

procedure TMQTTClient.SetLogger(const Logger: IMQTTLogger);
var
  NewLogger: IMQTTLogger;
begin
  if Assigned(Logger) then
    NewLogger := Logger
  else
    NewLogger := CreateNullLogger;

  // Interface assignment is not atomic (it AddRef/Releases). Without a lock
  // the receiver thread can observe a half-swapped FLogger and crash when
  // calling Debug on a freed instance. Snapshot+swap under the same lock
  // SnapshotLogger uses keeps the field consistent.
  FLock.Enter;
  try
    FLogger := NewLogger;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.GetLogger: IMQTTLogger;
begin
  FLock.Enter;
  try
    Result := FLogger;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.GetLastSessionPresent: Boolean;
begin
  // Atomic snapshot of the flag published by the most recent CONNACK
  // (both initial Connect and automatic reconnection).
  FLock.Enter;
  try
    Result := FLastSessionPresent;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.SnapshotLogger: IMQTTLogger;
begin
  // Internal accessor. Always returns a non-nil logger (constructor seeds a
  // null logger). Use this in any code path that may run concurrently with
  // SetLogger - notably the receiver task and the pinger task - to prevent
  // a torn interface read.
  FLock.Enter;
  try
    Result := FLogger;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.PacketTypeName(PT: TMQTTPacketType): string;
begin
  case PT of
    ptConnect:     Result := 'CONNECT';
    ptConnAck:     Result := 'CONNACK';
    ptPublish:     Result := 'PUBLISH';
    ptPubAck:      Result := 'PUBACK';
    ptPubRec:      Result := 'PUBREC';
    ptPubRel:      Result := 'PUBREL';
    ptPubComp:     Result := 'PUBCOMP';
    ptSubscribe:   Result := 'SUBSCRIBE';
    ptSubAck:      Result := 'SUBACK';
    ptUnsubscribe: Result := 'UNSUBSCRIBE';
    ptUnsubAck:    Result := 'UNSUBACK';
    ptPingReq:     Result := 'PINGREQ';
    ptPingResp:    Result := 'PINGRESP';
    ptDisconnect:  Result := 'DISCONNECT';
    ptAuth:        Result := 'AUTH';
  else
    Result := Format('UNKNOWN(%d)', [Ord(PT)]);
  end;
end;

procedure TMQTTClient.NotifyPacketSent(const Packet: TBytes);
var
  PT: TMQTTPacketType;
begin
  if Length(Packet) = 0 then
    Exit;
  PT := TMQTTProtocol.GetPacketType(Packet);
  SnapshotLogger.Debug('TX %s (%d bytes)', [PacketTypeName(PT), Length(Packet)]);
  if Assigned(FOnPacketSent) then
  begin
    try
      FOnPacketSent(PT, Packet);
    except
      // swallow handler exceptions
    end;
  end;
end;

procedure TMQTTClient.NotifyPacketReceived(const Packet: TBytes);
var
  PT: TMQTTPacketType;
begin
  if Length(Packet) = 0 then
    Exit;
  PT := TMQTTProtocol.GetPacketType(Packet);
  SnapshotLogger.Debug('RX %s (%d bytes)', [PacketTypeName(PT), Length(Packet)]);
  if Assigned(FOnPacketReceived) then
  begin
    try
      FOnPacketReceived(PT, Packet);
    except
      // swallow handler exceptions
    end;
  end;
end;

function TMQTTClient.GetSSLMethodVersion(Method: TMQTTSSLMethod): TIdSSLVersion;
begin
  case Method of
    sslTLS1:    Result := sslvTLSv1;
    sslTLS1_1:  Result := sslvTLSv1_1;
    sslTLS1_2:  Result := sslvTLSv1_2;
    sslTLS1_3:  Result := sslvTLSv1_2; // Fallback to 1.2, TLS 1.3 needs TaurusTLS
  else
    Result := sslvTLSv1_2; // sslAuto defaults to TLS 1.2
  end;
end;

procedure TMQTTClient.ConfigureSSL;
begin
  if not FSSLOptions.Enabled then
    Exit;

  // Clean up existing handler
  CleanupSSL;

  // Create new SSL handler
  FSSLHandler := TIdSSLIOHandlerSocketOpenSSL.Create(nil);
  FSSLHandler.SSLOptions.Method := GetSSLMethodVersion(FSSLOptions.Method);
  FSSLHandler.SSLOptions.Mode := sslmClient;

  // Configure verification mode
  case FSSLOptions.VerifyMode of
    sslVerifyNone:
      FSSLHandler.SSLOptions.VerifyMode := [];
    sslVerifyPeer:
      FSSLHandler.SSLOptions.VerifyMode := [sslvrfPeer];
  end;
  FSSLHandler.SSLOptions.VerifyDepth := FSSLOptions.VerifyDepth;

  // Configure certificates
  if FSSLOptions.CertFile <> '' then
    FSSLHandler.SSLOptions.CertFile := FSSLOptions.CertFile;
  if FSSLOptions.KeyFile <> '' then
    FSSLHandler.SSLOptions.KeyFile := FSSLOptions.KeyFile;
  if FSSLOptions.RootCertFile <> '' then
    FSSLHandler.SSLOptions.RootCertFile := FSSLOptions.RootCertFile;

  // Configure password callback if needed
  if FSSLOptions.KeyPassword <> '' then
    FSSLHandler.OnGetPassword := SSLGetPassword;

  // Attach handler to TCP client
  FIndy.IOHandler := FSSLHandler;

  // Enable SSL handshake (PassThrough = False means SSL is active)
  FSSLHandler.PassThrough := False;
end;

procedure TMQTTClient.CleanupSSL;
begin
  if Assigned(FSSLHandler) then
  begin
    if FIndy.IOHandler = FSSLHandler then
      FIndy.IOHandler := nil;
    FreeAndNil(FSSLHandler);
  end;
end;

procedure TMQTTClient.SSLGetPassword(var Password: string);
begin
  Password := FSSLOptions.KeyPassword;
end;

procedure TMQTTClient.Connect(const Host: string; Port: Word);
var
  Options: TMQTTConnectOptions;
begin
  Options.SetDefaults;
  Connect(Host, Port, Options);
end;

procedure TMQTTClient.Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions);
var
  Packet: TBytes;
  SessionPresent: Boolean;
  ReasonCode: Byte;
begin
  if FState <> Disconnected then
    Exit;

  FShutdown := False;
  FHost := Host;
  FPort := Port;
  FOptions := Options;
  FState := Connecting;

  try
    FIndy.Host := Host;
    FIndy.Port := Port;
    FIndy.Connect;

    Packet := TMQTTProtocol.BuildConnect(Options);
    SendPacket(Packet);

    FIndy.IOHandler.CheckForDataOnSource(5000);
    if FIndy.IOHandler.InputBufferIsEmpty then
    begin
      FIndy.Disconnect;
      FState := Disconnected;
      raise EMQTTConnectionException.Create('No CONNACK from broker');
    end;

    Packet := ReadPacket;
    NotifyPacketReceived(Packet);
    if not TMQTTProtocol.ParseConnAck(Packet, Ord(Options.Version), SessionPresent, ReasonCode) then
    begin
      FIndy.Disconnect;
      FState := Disconnected;
      SnapshotLogger.Error('Invalid CONNACK from %s:%d', [Host, Port]);
      raise EMQTTConnectionException.Create('Invalid CONNACK packet');
    end;

    if ReasonCode <> 0 then
    begin
      FIndy.Disconnect;
      FState := Disconnected;
      SnapshotLogger.Error('Connection refused by %s:%d (reason code %d)', [Host, Port, ReasonCode]);
      raise EMQTTConnectionException.CreateFmt('Connection refused: reason code %d', [ReasonCode]);
    end;

    FState := Connected;
    FLastActivity := Now;
    FLastSessionPresent := SessionPresent;
    if SessionPresent then
      SnapshotLogger.Info('Connected to %s:%d (ClientID="%s", MQTT v%d, SessionPresent=True)',
        [Host, Port, Options.ClientID, Ord(Options.Version)])
    else
      SnapshotLogger.Info('Connected to %s:%d (ClientID="%s", MQTT v%d, SessionPresent=False)',
        [Host, Port, Options.ClientID, Ord(Options.Version)]);

    // Start receiver thread
    FReceiver := TTask.Run(procedure begin ReceiverLoop; end);

    // Start keep-alive pinger if needed
    if Options.KeepAliveSec > 0 then
      FPinger := TTask.Run(procedure begin PingerLoop; end);

    if Assigned(FOnConnect) then
      FOnConnect(TMQTTReasonCode(ReasonCode), SessionPresent);

  except
    on E: Exception do
    begin
      FState := Disconnected;
      CleanupSSL;
      if FIndy.Connected then
        FIndy.Disconnect;
      raise;
    end;
  end;
end;

procedure TMQTTClient.Connect(const Host: string; Port: Word; const Options: TMQTTConnectOptions;
  const SSLOptions: TMQTTSSLOptions);
begin
  FSSLOptions := SSLOptions;
  if FSSLOptions.Enabled then
    ConfigureSSL;
  Connect(Host, Port, Options);
end;

procedure TMQTTClient.ConnectSSL(const Host: string; Port: Word);
var
  Options: TMQTTConnectOptions;
  SSLOpts: TMQTTSSLOptions;
begin
  Options.SetDefaults;
  SSLOpts.SetDefaults;
  SSLOpts.Enabled := True;
  Connect(Host, Port, Options, SSLOpts);
end;

procedure TMQTTClient.ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions);
var
  SSLOpts: TMQTTSSLOptions;
begin
  SSLOpts.SetDefaults;
  SSLOpts.Enabled := True;
  Connect(Host, Port, Options, SSLOpts);
end;

procedure TMQTTClient.ConnectSSL(const Host: string; Port: Word; const Options: TMQTTConnectOptions;
  const SSLOptions: TMQTTSSLOptions);
var
  SSLOpts: TMQTTSSLOptions;
begin
  SSLOpts := SSLOptions;
  SSLOpts.Enabled := True; // Force SSL enabled for ConnectSSL
  Connect(Host, Port, Options, SSLOpts);
end;

procedure TMQTTClient.Disconnect;
var
  Packet: TBytes;
begin
  FShutdown := True;

  if FState = Disconnected then
    Exit;

  SnapshotLogger.Info('Disconnecting from %s:%d', [FHost, FPort]);

  // Send DISCONNECT packet
  if FIndy.Connected then
  begin
    try
      Packet := TMQTTProtocol.BuildDisconnect(Ord(FOptions.Version));
      SendPacket(Packet);
    except
      // Ignore send errors during disconnect
    end;
  end;

  FState := Disconnected;

  if FIndy.Connected then
    FIndy.Disconnect;

  // Wait for threads to finish
  if Assigned(FPinger) then
  begin
    FPinger.Wait(2000);
    FPinger := nil;
  end;

  if Assigned(FReceiver) then
  begin
    FReceiver.Wait(2000);
    FReceiver := nil;
  end;

  // Clear pending operations
  FLock.Enter;
  try
    FPendingPublish.Clear;
    FPendingQoS2Inbound.Clear;
  finally
    FLock.Leave;
  end;
end;

procedure TMQTTClient.SendPacket(const Packet: TBytes);
begin
  FWriteLock.Enter;
  try
    if FIndy.Connected then
    begin
      FIndy.IOHandler.Write(TIdBytes(Packet), Length(Packet));
      FLastActivity := Now;
    end;
  finally
    FWriteLock.Leave;
  end;
  NotifyPacketSent(Packet);
end;

function TMQTTClient.ReadPacket: TBytes;
var
  H: Byte;
  RL, Multiplier, Digit: Integer;
  Body: TIdBytes;
  RLBytes: TBytes;
begin
  SetLength(Result, 0);

  H := FIndy.IOHandler.ReadByte;
  Multiplier := 1;
  RL := 0;

  repeat
    Digit := FIndy.IOHandler.ReadByte;
    RL := RL + (Digit and 127) * Multiplier;
    if Multiplier > 128 * 128 * 128 then
      raise EMQTTProtocolException.Create('Remaining length too large');
    Multiplier := Multiplier * 128;
  until (Digit and 128) = 0;

  RLBytes := TMQTTProtocol.EncodeLen(RL);
  SetLength(Result, 1 + Length(RLBytes) + RL);
  Result[0] := H;
  Move(RLBytes[0], Result[1], Length(RLBytes));

  if RL > 0 then
  begin
    FIndy.IOHandler.ReadBytes(Body, RL);
    Move(Body[0], Result[1 + Length(RLBytes)], RL);
  end;

  FLastActivity := Now;
end;

procedure TMQTTClient.ReceiverLoop;
var
  Packet: TBytes;
begin
  while not FShutdown and (FState in [Connected, Reconnecting]) do
  begin
    try
      if not FIndy.Connected then
      begin
        if FAutoReconnect and not FShutdown then
          DoReconnect
        else
          Break;
        Continue;
      end;

      FIndy.IOHandler.CheckForDataOnSource(100);
      if FIndy.IOHandler.InputBufferIsEmpty then
      begin
        RetryPendingPublishes;
        Continue;
      end;

      Packet := ReadPacket;
      if Length(Packet) > 0 then
      begin
        NotifyPacketReceived(Packet);
        HandlePacket(Packet);
      end;

    except
      on E: EIdReadTimeout do
        Continue;
      on E: EIdConnClosedGracefully do
      begin
        if FAutoReconnect and not FShutdown then
          DoReconnect
        else
        begin
          DoDisconnect(rcSuccess, 'Connection closed by broker');
          Break;
        end;
      end;
      on E: Exception do
      begin
        DoError(E.Message);
        if FAutoReconnect and not FShutdown then
          DoReconnect
        else
        begin
          DoDisconnect(rcUnspecifiedError, E.Message);
          Break;
        end;
      end;
    end;
  end;

  FState := Disconnected;
end;

procedure TMQTTClient.PingerLoop;
var
  KeepAliveInterval: Double;
  Packet: TBytes;
begin
  KeepAliveInterval := FOptions.KeepAliveSec * 0.75; // Send ping at 75% of keep-alive

  while not FShutdown and (FState = Connected) do
  begin
    Sleep(1000);

    if FShutdown or (FState <> Connected) then
      Break;

    // Check if we need to send a ping
    if SecondsBetween(Now, FLastActivity) >= KeepAliveInterval then
    begin
      try
        Packet := TMQTTProtocol.BuildPingReq;
        SendPacket(Packet);
      except
        // Ignore ping errors
      end;
    end;
  end;
end;

procedure TMQTTClient.HandlePacket(const Packet: TBytes);
var
  PacketType: TMQTTPacketType;
begin
  if Length(Packet) = 0 then
    Exit;

  PacketType := TMQTTProtocol.GetPacketType(Packet);

  case PacketType of
    ptPublish:
      HandlePublish(Packet);
    ptPubAck:
      HandlePubAck(Packet);
    ptPubRec:
      HandlePubRec(Packet);
    ptPubRel:
      HandlePubRel(Packet);
    ptPubComp:
      HandlePubComp(Packet);
    ptSubAck:
      HandleSubAck(Packet);
    ptUnsubAck:
      HandleUnsubAck(Packet);
    ptPingResp:
      HandlePingResp;
    ptDisconnect:
      HandleDisconnect(Packet);
  end;
end;

procedure TMQTTClient.HandlePublish(const Packet: TBytes);
var
  Topic: string;
  Payload: TBytes;
  QoS: TMQTTQoS;
  Retain, Dup: Boolean;
  PacketID: Word;
  PendingMsg: TPendingQoS2Inbound;
  HasManualAck, ShouldAck: Boolean;
begin
  if not TMQTTProtocol.ParsePublish(Packet, Ord(FOptions.Version), Topic, Payload, QoS, Retain, Dup, PacketID) then
    Exit;

  // Check if any matching subscription uses ManualAck
  HasManualAck := HasManualAckSubscription(Topic);

  case QoS of
    atMostOnce:
      begin
        // No ACK needed for QoS 0
        if HasManualAck then
          DispatchMessageManualAck(Topic, Payload, Dup, QoS, 0)
        else
          DispatchMessageEx(Topic, Payload, Dup, QoS, 0);
      end;

    atLeastOnce:
      begin
        if HasManualAck then
        begin
          // Manual ACK - let handler decide
          ShouldAck := DispatchMessageManualAck(Topic, Payload, Dup, QoS, PacketID);
          if ShouldAck then
            SendAck(QoS, PacketID);
        end
        else
        begin
          // Auto ACK - dispatch first, then ACK. This preserves the
          // "at-least-once to application" guarantee: if the process dies
          // between receive and ACK, the broker redelivers. If we ACK'd
          // first, a crash mid-dispatch would silently lose the message.
          DispatchMessageEx(Topic, Payload, Dup, QoS, PacketID);
          SendAck(QoS, PacketID);
        end;
      end;

    exactlyOnce:
      begin
        // Store message with topic for later dispatch
        PendingMsg.Topic := Topic;
        PendingMsg.Payload := Payload;
        PendingMsg.Dup := Dup;
        FLock.Enter;
        try
          FPendingQoS2Inbound.AddOrSetValue(PacketID, PendingMsg);
        finally
          FLock.Leave;
        end;
        // Always send PUBREC - manual ACK decision happens at PUBREL
        SendAck(QoS, PacketID);
      end;
  end;
end;

procedure TMQTTClient.HandlePubAck(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCode: Byte;
  Pending: TPendingPublish;
  Found: Boolean;
begin
  if not TMQTTProtocol.ParsePubAck(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;

  FLock.Enter;
  try
    Found := FPendingPublish.TryGetValue(PacketID, Pending);
    if Found then
      FPendingPublish.Remove(PacketID);
  finally
    FLock.Leave;
  end;

  // Signal the specific PublishSync waiter for this packet, if any.
  if Found and Assigned(Pending.AckEvent) then
    Pending.AckEvent.SetEvent;
end;

procedure TMQTTClient.HandlePubRec(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCode: Byte;
  Pending: TPendingPublish;
  RelPacket: TBytes;
begin
  if not TMQTTProtocol.ParsePubRec(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;

  FLock.Enter;
  try
    if FPendingPublish.TryGetValue(PacketID, Pending) then
    begin
      Pending.State := psWaitingPubComp;
      FPendingPublish[PacketID] := Pending;
    end;
  finally
    FLock.Leave;
  end;

  // Send PUBREL
  RelPacket := TMQTTProtocol.BuildPubRel(Ord(FOptions.Version), PacketID);
  SendPacket(RelPacket);
end;

procedure TMQTTClient.HandlePubRel(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCode: Byte;
  PendingMsg: TPendingQoS2Inbound;
  CompPacket: TBytes;
  ShouldDispatch, HasManualAck, ShouldAck: Boolean;
begin
  if not TMQTTProtocol.ParsePubRel(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;

  // Get stored message under lock
  ShouldDispatch := False;
  FLock.Enter;
  try
    if FPendingQoS2Inbound.TryGetValue(PacketID, PendingMsg) then
    begin
      FPendingQoS2Inbound.Remove(PacketID);
      ShouldDispatch := True;
    end;
  finally
    FLock.Leave;
  end;

  // Dispatch outside lock
  ShouldAck := True; // Default: send PUBCOMP
  if ShouldDispatch then
  begin
    HasManualAck := HasManualAckSubscription(PendingMsg.Topic);
    if HasManualAck then
      ShouldAck := DispatchMessageManualAck(PendingMsg.Topic, PendingMsg.Payload, PendingMsg.Dup, exactlyOnce, PacketID)
    else
      DispatchMessageEx(PendingMsg.Topic, PendingMsg.Payload, PendingMsg.Dup, exactlyOnce, PacketID);
  end;

  // Send PUBCOMP only if acknowledged
  if ShouldAck then
  begin
    CompPacket := TMQTTProtocol.BuildPubComp(Ord(FOptions.Version), PacketID);
    SendPacket(CompPacket);
  end;
end;

procedure TMQTTClient.HandlePubComp(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCode: Byte;
  Pending: TPendingPublish;
  Found: Boolean;
begin
  if not TMQTTProtocol.ParsePubComp(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;

  FLock.Enter;
  try
    Found := FPendingPublish.TryGetValue(PacketID, Pending);
    if Found then
      FPendingPublish.Remove(PacketID);
  finally
    FLock.Leave;
  end;

  if Found and Assigned(Pending.AckEvent) then
    Pending.AckEvent.SetEvent;
end;

procedure TMQTTClient.HandleSubAck(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCodes: TArray<Byte>;
  I: Integer;
  CodesStr: string;
begin
  if not TMQTTProtocol.ParseSubAck(Packet, Ord(FOptions.Version), PacketID, ReasonCodes) then
    Exit;

  if SnapshotLogger.MinLevel <= llInfo then
  begin
    CodesStr := '';
    for I := 0 to High(ReasonCodes) do
    begin
      if I > 0 then CodesStr := CodesStr + ',';
      CodesStr := CodesStr + IntToStr(ReasonCodes[I]);
    end;
    SnapshotLogger.Info('SUBACK PacketID=%d GrantedQoS=[%s]', [PacketID, CodesStr]);
  end;

  if Assigned(FOnSubscribeAck) then
  begin
    try
      FOnSubscribeAck(PacketID, ReasonCodes);
    except
      // swallow handler exceptions
    end;
  end;
end;

procedure TMQTTClient.HandleUnsubAck(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCodes: TArray<Byte>;
begin
  TMQTTProtocol.ParseUnsubAck(Packet, Ord(FOptions.Version), PacketID, ReasonCodes);
  // Unsubscription confirmed
end;

procedure TMQTTClient.HandlePingResp;
begin
  // Ping response received, connection is alive
  FLastActivity := Now;
end;

procedure TMQTTClient.HandleDisconnect(const Packet: TBytes);
var
  Offset: Integer;
  ReasonCode: Byte;
begin
  ReasonCode := 0;
  if Length(Packet) > 2 then
  begin
    Offset := 1;
    TMQTTProtocol.DecodeLen(Packet, Offset);
    if Offset < Length(Packet) then
      ReasonCode := Packet[Offset];
  end;

  // Broker initiated DISCONNECT: close the socket here. Without this the
  // OS socket stays open while FState moves to Disconnected, the receiver
  // loop next iteration calls ReadByte on a half-closed connection and
  // either blocks or raises a confusing exception.
  if FIndy.Connected then
  begin
    try
      FIndy.Disconnect;
    except
      // swallow - we are already tearing down
    end;
  end;

  DoDisconnect(TMQTTReasonCode(ReasonCode), 'Disconnect from broker');
end;

procedure TMQTTClient.DispatchMessageEx(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS; PacketID: Word);
var
  SubInfo: TSubscriptionInfo;
  Filter: string;
  SimpleHandlers: TArray<TMQTTHandler>;
  ExtendedHandlers: TArray<TMQTTExtendedHandler>;
  Handler: TMQTTHandler;
  ExtHandler: TMQTTExtendedHandler;
begin
  // Collect matching handlers under lock (htSimple and htExtended only)
  SetLength(SimpleHandlers, 0);
  SetLength(ExtendedHandlers, 0);

  FLock.Enter;
  try
    // First try exact match
    if FSubscriptions.TryGetValue(Topic, SubInfo) then
    begin
      case SubInfo.HandlerType of
        htSimple:
          begin
            SetLength(SimpleHandlers, 1);
            SimpleHandlers[0] := SubInfo.Handler;
          end;
        htExtended:
          begin
            SetLength(ExtendedHandlers, 1);
            ExtendedHandlers[0] := SubInfo.ExtendedHandler;
          end;
      end;
    end
    else
    begin
      // Try wildcard matching
      for Filter in FSubscriptions.Keys do
      begin
        if TMQTTProtocol.TopicMatchesFilter(Topic, Filter) then
        begin
          SubInfo := FSubscriptions[Filter];
          case SubInfo.HandlerType of
            htSimple:
              begin
                SetLength(SimpleHandlers, Length(SimpleHandlers) + 1);
                SimpleHandlers[High(SimpleHandlers)] := SubInfo.Handler;
              end;
            htExtended:
              begin
                SetLength(ExtendedHandlers, Length(ExtendedHandlers) + 1);
                ExtendedHandlers[High(ExtendedHandlers)] := SubInfo.ExtendedHandler;
              end;
          end;
        end;
      end;
    end;
  finally
    FLock.Leave;
  end;

  // Call simple handlers outside lock
  for Handler in SimpleHandlers do
  begin
    try
      Handler(Topic, Payload);
    except
      // Ignore exceptions in handlers
    end;
  end;

  // Call extended handlers outside lock
  for ExtHandler in ExtendedHandlers do
  begin
    try
      ExtHandler(Topic, Payload, Dup, QoS);
    except
      // Ignore exceptions in handlers
    end;
  end;
end;

function TMQTTClient.DispatchMessageManualAck(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS; PacketID: Word): Boolean;
var
  SubInfo: TSubscriptionInfo;
  Filter: string;
  ManualHandlers: TArray<TMQTTManualAckHandler>;
  ManualHandler: TMQTTManualAckHandler;
  Ack: Boolean;
begin
  Result := True; // Default: acknowledge

  // Collect matching ManualAck handlers under lock
  SetLength(ManualHandlers, 0);

  FLock.Enter;
  try
    // First try exact match
    if FSubscriptions.TryGetValue(Topic, SubInfo) then
    begin
      if SubInfo.HandlerType = htManualAck then
      begin
        SetLength(ManualHandlers, 1);
        ManualHandlers[0] := SubInfo.ManualAckHandler;
      end;
    end
    else
    begin
      // Try wildcard matching
      for Filter in FSubscriptions.Keys do
      begin
        if TMQTTProtocol.TopicMatchesFilter(Topic, Filter) then
        begin
          SubInfo := FSubscriptions[Filter];
          if SubInfo.HandlerType = htManualAck then
          begin
            SetLength(ManualHandlers, Length(ManualHandlers) + 1);
            ManualHandlers[High(ManualHandlers)] := SubInfo.ManualAckHandler;
          end;
        end;
      end;
    end;
  finally
    FLock.Leave;
  end;

  // Call handlers outside lock
  // If ANY handler returns Ack=False, don't acknowledge
  for ManualHandler in ManualHandlers do
  begin
    try
      Ack := False; // Default: don't ack (safer)
      ManualHandler(Topic, Payload, Dup, Ack);
      if not Ack then
        Result := False;
    except
      // Exception = don't acknowledge (will be redelivered)
      Result := False;
    end;
  end;
end;

function TMQTTClient.HasManualAckSubscription(const Topic: string): Boolean;
var
  SubInfo: TSubscriptionInfo;
  Filter: string;
begin
  Result := False;

  FLock.Enter;
  try
    // Check exact match
    if FSubscriptions.TryGetValue(Topic, SubInfo) then
    begin
      if SubInfo.HandlerType = htManualAck then
        Exit(True);
    end;

    // Check wildcard matches
    for Filter in FSubscriptions.Keys do
    begin
      if TMQTTProtocol.TopicMatchesFilter(Topic, Filter) then
      begin
        SubInfo := FSubscriptions[Filter];
        if SubInfo.HandlerType = htManualAck then
          Exit(True);
      end;
    end;
  finally
    FLock.Leave;
  end;
end;

procedure TMQTTClient.SendAck(QoS: TMQTTQoS; PacketID: Word);
var
  AckPacket: TBytes;
begin
  case QoS of
    atLeastOnce:
      begin
        AckPacket := TMQTTProtocol.BuildPubAck(Ord(FOptions.Version), PacketID);
        SendPacket(AckPacket);
      end;
    exactlyOnce:
      begin
        AckPacket := TMQTTProtocol.BuildPubRec(Ord(FOptions.Version), PacketID);
        SendPacket(AckPacket);
      end;
  end;
end;

procedure TMQTTClient.DoReconnect;
var
  Delay: Integer;
begin
  if FShutdown then
    Exit;

  FState := Reconnecting;
  Delay := FReconnectDelayMs;
  SnapshotLogger.Warning('Connection lost, attempting reconnect to %s:%d', [FHost, FPort]);

  // Release the old pinger reference. Its loop already exited because
  // FState <> Connected, but the ITask reference stays assigned until
  // explicitly cleared, which would prevent the post-reconnect restart.
  if Assigned(FPinger) then
  begin
    try
      FPinger.Wait(500);
    except
      // ignore - we're tearing down the old one anyway
    end;
    FPinger := nil;
  end;

  while not FShutdown and (FState = Reconnecting) do
  begin
    Sleep(Delay);

    if FShutdown then
      Break;

    try
      if FIndy.Connected then
        FIndy.Disconnect;

      // Reconfigure SSL if enabled
      if FSSLOptions.Enabled then
        ConfigureSSL;

      FIndy.Host := FHost;
      FIndy.Port := FPort;
      FIndy.Connect;

      // Re-send CONNECT
      var Packet := TMQTTProtocol.BuildConnect(FOptions);
      SendPacket(Packet);

      FIndy.IOHandler.CheckForDataOnSource(5000);
      if not FIndy.IOHandler.InputBufferIsEmpty then
      begin
        Packet := ReadPacket;
        NotifyPacketReceived(Packet);
        var SessionPresent: Boolean;
        var ReasonCode: Byte;
        if TMQTTProtocol.ParseConnAck(Packet, Ord(FOptions.Version), SessionPresent, ReasonCode) and (ReasonCode = 0) then
        begin
          FState := Connected;
          FLastActivity := Now;
          FLastSessionPresent := SessionPresent;
          SnapshotLogger.Info('Reconnected to %s:%d (SessionPresent=%s)',
            [FHost, FPort, BoolToStr(SessionPresent, True)]);

          // Restart pinger (the old one was nil'd at the top of DoReconnect)
          if FOptions.KeepAliveSec > 0 then
            FPinger := TTask.Run(procedure begin PingerLoop; end);

          // Re-send SUBSCRIBE for every active subscription. With
          // CleanStart = True (the default) the broker started a new
          // session and forgot our subscriptions, so messages would
          // silently stop flowing without this step.
          ResubscribeAll;

          // Retry pending publishes
          RetryPendingPublishes;

          if Assigned(FOnConnect) then
            FOnConnect(TMQTTReasonCode(ReasonCode), SessionPresent);

          Exit;
        end;
      end;

    except
      // Reconnection failed, try again
    end;

    // Exponential backoff
    Delay := Min(Delay * 2, FMaxReconnectDelayMs);
  end;
end;

procedure TMQTTClient.DoDisconnect(ReasonCode: TMQTTReasonCode; const ReasonString: string);
begin
  FState := Disconnected;

  SnapshotLogger.Info('Disconnected: reason=%d (%s)', [Ord(ReasonCode), ReasonString]);

  if Assigned(FOnDisconnect) then
    FOnDisconnect(ReasonCode, ReasonString);
end;

procedure TMQTTClient.DoError(const Msg: string);
begin
  SnapshotLogger.Error(Msg);
  if Assigned(FOnError) then
    FOnError(Msg);
end;

procedure TMQTTClient.ResubscribeAll;
var
  Pair: TPair<string, TSubscriptionInfo>;
  Snapshot: TArray<TPair<string, TSubscriptionInfo>>;
  PacketID: Word;
  Packet: TBytes;
begin
  // Take a snapshot under lock so we can send SUBSCRIBE packets without
  // holding FLock (SendPacket grabs FWriteLock and the logger).
  FLock.Enter;
  try
    SetLength(Snapshot, FSubscriptions.Count);
    var I := 0;
    for Pair in FSubscriptions do
    begin
      Snapshot[I] := Pair;
      Inc(I);
    end;
  finally
    FLock.Leave;
  end;

  for Pair in Snapshot do
  begin
    // Skip entries created via RestoreSubscriptionHandler: their original QoS
    // is unknown, and the broker-side subscription is expected to survive
    // either via session persistence (LastSessionPresent=True after reconnect)
    // or to be re-established explicitly by the caller. Issuing a SUBSCRIBE
    // with a guessed QoS could silently override the original one.
    if Pair.Value.Bound then
    begin
      SnapshotLogger.Warning('Skipping RE-SUBSCRIBE for bound-only topic "%s" ' +
        '(original QoS unknown). If the broker session was lost, call Subscribe ' +
        'explicitly.', [Pair.Key]);
      Continue;
    end;

    PacketID := GetNextPacketID;
    Packet := TMQTTProtocol.BuildSubscribe(Ord(FOptions.Version), PacketID,
      Pair.Key, Pair.Value.QoS);
    SnapshotLogger.Info('RE-SUBSCRIBE PacketID=%d topic="%s" qos=%d',
      [PacketID, Pair.Key, Ord(Pair.Value.QoS)]);
    try
      SendPacket(Packet);
    except
      on E: Exception do
        SnapshotLogger.Error('Re-subscribe failed for "%s": %s', [Pair.Key, E.Message]);
    end;
  end;
end;

procedure TMQTTClient.RetryPendingPublishes;
var
  Pair: TPair<Word, TPendingPublish>;
  Pending: TPendingPublish;
  Packet: TBytes;
  ToRetry: TList<TPendingPublish>;
begin
  if FState <> Connected then
    Exit;

  ToRetry := TList<TPendingPublish>.Create;
  try
    FLock.Enter;
    try
      for Pair in FPendingPublish do
      begin
        if SecondsBetween(Now, Pair.Value.Timestamp) > 5 then
          ToRetry.Add(Pair.Value);
      end;
    finally
      FLock.Leave;
    end;

    for Pending in ToRetry do
    begin
      if Pending.RetryCount >= 3 then
      begin
        FLock.Enter;
        try
          FPendingPublish.Remove(Pending.PacketID);
        finally
          FLock.Leave;
        end;
        SnapshotLogger.Warning(
          'Pending publish dropped after 3 retries: PacketID=%d topic="%s" qos=%d',
          [Pending.PacketID, Pending.Topic, Ord(Pending.QoS)]);
        DoError(Format('Publish dropped after retries: PacketID=%d topic="%s"',
          [Pending.PacketID, Pending.Topic]));
        // Wake any PublishSync caller waiting on this entry so it returns
        // False instead of timing out after a long wait.
        if Assigned(Pending.AckEvent) then
          Pending.AckEvent.SetEvent;
        Continue;
      end;

      case Pending.State of
        psPending:
          begin
            Packet := TMQTTProtocol.BuildPublish(Ord(FOptions.Version), Pending.Topic,
              Pending.Payload, Pending.QoS, Pending.Retain, True, Pending.PacketID);
            SendPacket(Packet);
          end;
        psWaitingPubComp:
          begin
            Packet := TMQTTProtocol.BuildPubRel(Ord(FOptions.Version), Pending.PacketID);
            SendPacket(Packet);
          end;
      end;

      FLock.Enter;
      try
        var UpdatedPending: TPendingPublish;
        if FPendingPublish.TryGetValue(Pending.PacketID, UpdatedPending) then
        begin
          UpdatedPending.Timestamp := Now;
          Inc(UpdatedPending.RetryCount);
          FPendingPublish[Pending.PacketID] := UpdatedPending;
        end;
      finally
        FLock.Leave;
      end;
    end;
  finally
    ToRetry.Free;
  end;
end;

procedure TMQTTClient.Publish(const Topic: string; const Payload: string; QoS: TMQTTQoS; Retain: Boolean);
begin
  Publish(Topic, TEncoding.UTF8.GetBytes(Payload), QoS, Retain);
end;

procedure TMQTTClient.Publish(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS; Retain: Boolean);
var
  PacketID: Word;
  Packet: TBytes;
  Pending: TPendingPublish;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if not TMQTTProtocol.IsValidTopicName(Topic) then
    raise EMQTTException.Create('Invalid topic name');

  if QoS = atMostOnce then
    PacketID := 0
  else
    PacketID := GetNextPacketID;

  Packet := TMQTTProtocol.BuildPublish(Ord(FOptions.Version), Topic, Payload, QoS, Retain, False, PacketID);
  SendPacket(Packet);

  // Store for QoS 1/2
  if QoS > atMostOnce then
  begin
    Pending.PacketID := PacketID;
    Pending.Topic := Topic;
    Pending.Payload := Copy(Payload);
    Pending.QoS := QoS;
    Pending.Retain := Retain;
    Pending.Timestamp := Now;
    Pending.RetryCount := 0;
    Pending.State := psPending;
    Pending.AckEvent := nil; // fire-and-forget Publish doesn't wait

    FLock.Enter;
    try
      FPendingPublish.Add(PacketID, Pending);
    finally
      FLock.Leave;
    end;
  end;
end;

function TMQTTClient.PublishSync(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS;
  Retain: Boolean; TimeoutMs: Cardinal): Boolean;
var
  PacketID: Word;
  Packet: TBytes;
  Pending: TPendingPublish;
  WaitRes: TWaitResult;
  PerCallEvent: TEvent;
begin
  Result := False;

  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if QoS = atMostOnce then
  begin
    Publish(Topic, Payload, QoS, Retain);
    Result := True;
    Exit;
  end;

  PacketID := GetNextPacketID;
  Packet := TMQTTProtocol.BuildPublish(Ord(FOptions.Version), Topic, Payload, QoS, Retain, False, PacketID);

  // Dedicated per-call event. Without this, two concurrent PublishSync
  // calls would share one auto-reset TEvent and the wrong one could wake
  // up - the other would then time out even though its ACK arrived.
  PerCallEvent := TEvent.Create(nil, False, False, '');
  try
    Pending.PacketID := PacketID;
    Pending.Topic := Topic;
    Pending.Payload := Copy(Payload);
    Pending.QoS := QoS;
    Pending.Retain := Retain;
    Pending.Timestamp := Now;
    Pending.RetryCount := 0;
    Pending.State := psPending;
    Pending.AckEvent := PerCallEvent;

    FLock.Enter;
    try
      FPendingPublish.Add(PacketID, Pending);
    finally
      FLock.Leave;
    end;

    SendPacket(Packet);

    WaitRes := PerCallEvent.WaitFor(TimeoutMs);

    FLock.Enter;
    try
      // Whether we timed out or got signaled, scrub the pending entry so
      // a late ACK can't reference a freed event.
      if FPendingPublish.ContainsKey(PacketID) then
      begin
        var Stale: TPendingPublish;
        if FPendingPublish.TryGetValue(PacketID, Stale) then
          Stale.AckEvent := nil; // detach before removing
        FPendingPublish.Remove(PacketID);
      end
      else
        Result := True; // ACK already removed it
    finally
      FLock.Leave;
    end;

    // WaitRes wrSignaled may race with the "already removed" branch above;
    // either is OK - both mean we got the ACK.
    if WaitRes = wrSignaled then
      Result := True;
  finally
    PerCallEvent.Free;
  end;
end;

procedure TMQTTClient.Subscribe(const Topic: string; Handler: TMQTTHandler; QoS: TMQTTQoS);
var
  PacketID: Word;
  Packet: TBytes;
  SubInfo: TSubscriptionInfo;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if not TMQTTProtocol.IsValidTopicFilter(Topic) then
    raise EMQTTException.Create('Invalid topic filter');

  SubInfo.Handler := Handler;
  SubInfo.ExtendedHandler := nil;
  SubInfo.ManualAckHandler := nil;
  SubInfo.HandlerType := htSimple;
  SubInfo.QoS := QoS;
  SubInfo.Bound := False;

  FLock.Enter;
  try
    FSubscriptions.AddOrSetValue(Topic, SubInfo);
  finally
    FLock.Leave;
  end;

  PacketID := GetNextPacketID;
  Packet := TMQTTProtocol.BuildSubscribe(Ord(FOptions.Version), PacketID, Topic, QoS);
  SnapshotLogger.Info('SUBSCRIBE PacketID=%d topic="%s" qos=%d', [PacketID, Topic, Ord(QoS)]);
  SendPacket(Packet);
end;

procedure TMQTTClient.RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTHandler);
var
  SubInfo: TSubscriptionInfo;
begin
  // Register only the local dispatcher for a topic whose subscription already
  // exists broker-side (typical after Connect with LastSessionPresent=True).
  // Does NOT send SUBSCRIBE: no round-trip, no SUBACK, no PacketID consumed.
  // The effective QoS of received PUBLISH packets is the one stored by the
  // broker for the original subscription, applied by the wire packet parser.
  //
  // Can be called BEFORE Connect (recommended). The broker may start delivering
  // queued messages immediately after CONNACK, so binding the handler upfront
  // avoids a race where the first PUBLISH arrives before the dispatcher exists.
  if not TMQTTProtocol.IsValidTopicFilter(Topic) then
    raise EMQTTException.Create('Invalid topic filter');

  SubInfo.Handler := Handler;
  SubInfo.ExtendedHandler := nil;
  SubInfo.ManualAckHandler := nil;
  SubInfo.HandlerType := htSimple;
  SubInfo.QoS := atMostOnce; // placeholder: not used while Bound=True
  SubInfo.Bound := True;

  FLock.Enter;
  try
    FSubscriptions.AddOrSetValue(Topic, SubInfo);
  finally
    FLock.Leave;
  end;

  SnapshotLogger.Info('RESTORE handler topic="%s" (no wire SUBSCRIBE)', [Topic]);
end;

procedure TMQTTClient.RestoreSubscriptionHandler(const Topic: string; Handler: TMQTTMessageEvent);
begin
  RestoreSubscriptionHandler(Topic,
    procedure(const ATopic: string; const APayload: TBytes)
    begin
      Handler(ATopic, APayload);
    end);
end;

procedure TMQTTClient.Subscribe(const Topic: string; Handler: TMQTTMessageEvent; QoS: TMQTTQoS);
begin
  // Wrap method pointer in anonymous method
  Subscribe(Topic,
    procedure(const ATopic: string; const APayload: TBytes)
    begin
      Handler(ATopic, APayload);
    end,
    QoS);
end;

procedure TMQTTClient.SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckHandler; QoS: TMQTTQoS);
var
  PacketID: Word;
  Packet: TBytes;
  SubInfo: TSubscriptionInfo;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if not TMQTTProtocol.IsValidTopicFilter(Topic) then
    raise EMQTTException.Create('Invalid topic filter');

  SubInfo.Handler := nil;
  SubInfo.ExtendedHandler := nil;
  SubInfo.ManualAckHandler := Handler;
  SubInfo.HandlerType := htManualAck;
  SubInfo.QoS := QoS;
  SubInfo.Bound := False;

  FLock.Enter;
  try
    FSubscriptions.AddOrSetValue(Topic, SubInfo);
  finally
    FLock.Leave;
  end;

  PacketID := GetNextPacketID;
  Packet := TMQTTProtocol.BuildSubscribe(Ord(FOptions.Version), PacketID, Topic, QoS);
  SendPacket(Packet);
end;

procedure TMQTTClient.SubscribeManualAck(const Topic: string; Handler: TMQTTManualAckEvent; QoS: TMQTTQoS);
begin
  // Wrap method pointer in anonymous method
  SubscribeManualAck(Topic,
    procedure(const ATopic: string; const APayload: TBytes; ADup: Boolean; var Ack: Boolean)
    begin
      Handler(ATopic, APayload, ADup, Ack);
    end,
    QoS);
end;

procedure TMQTTClient.SubscribeEx(const Topic: string; Handler: TMQTTExtendedHandler; QoS: TMQTTQoS);
var
  PacketID: Word;
  Packet: TBytes;
  SubInfo: TSubscriptionInfo;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if not TMQTTProtocol.IsValidTopicFilter(Topic) then
    raise EMQTTException.Create('Invalid topic filter');

  SubInfo.Handler := nil;
  SubInfo.ExtendedHandler := Handler;
  SubInfo.ManualAckHandler := nil;
  SubInfo.HandlerType := htExtended;
  SubInfo.QoS := QoS;
  SubInfo.Bound := False;

  FLock.Enter;
  try
    FSubscriptions.AddOrSetValue(Topic, SubInfo);
  finally
    FLock.Leave;
  end;

  PacketID := GetNextPacketID;
  Packet := TMQTTProtocol.BuildSubscribe(Ord(FOptions.Version), PacketID, Topic, QoS);
  SendPacket(Packet);
end;

procedure TMQTTClient.SubscribeEx(const Topic: string; Handler: TMQTTExtendedEvent; QoS: TMQTTQoS);
begin
  // Wrap method pointer in anonymous method
  SubscribeEx(Topic,
    procedure(const ATopic: string; const APayload: TBytes; ADup: Boolean; AQoS: TMQTTQoS)
    begin
      Handler(ATopic, APayload, ADup, AQoS);
    end,
    QoS);
end;

procedure TMQTTClient.Unsubscribe(const Topic: string);
var
  PacketID: Word;
  Packet: TBytes;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  FLock.Enter;
  try
    FSubscriptions.Remove(Topic);
  finally
    FLock.Leave;
  end;

  PacketID := GetNextPacketID;
  Packet := TMQTTProtocol.BuildUnsubscribe(Ord(FOptions.Version), PacketID, [Topic]);
  SendPacket(Packet);
end;

end.
