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
    State: (psPending, psWaitingPubRec, psWaitingPubComp, psFailed);
    // Per-publish wakeup signal. Non-nil only for PublishSync callers.
    // Signalled under FLock by CompletePending: on success the entry is
    // removed, on failure it stays with State = psFailed and PublishSync
    // removes it. NEVER share between calls.
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
    procedure SetReconnectInitialDelayMs(Value: Integer);
    function GetReconnectInitialDelayMs: Integer;
    procedure SetReconnectMaxDelayMs(Value: Integer);
    function GetReconnectMaxDelayMs: Integer;
    procedure SetReconnectJitterPercent(Value: Integer);
    function GetReconnectJitterPercent: Integer;
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
    // Auto-reconnect backoff parameters (only relevant when AutoReconnect=True).
    // Default backoff sequence with InitialDelay=1000, MaxDelay=30000, Jitter=25:
    //   ~1s, ~2s, ~4s, ~8s, ~16s, then ~30s capped. Each delay is multiplied
    //   by a random factor in [1 - JitterPercent/100, 1 + JitterPercent/100]
    //   to avoid thundering-herd reconnects when many clients lose the broker
    //   simultaneously. Set ReconnectJitterPercent=0 to disable jitter.
    property ReconnectInitialDelayMs: Integer
      read GetReconnectInitialDelayMs write SetReconnectInitialDelayMs;
    property ReconnectMaxDelayMs: Integer
      read GetReconnectMaxDelayMs write SetReconnectMaxDelayMs;
    property ReconnectJitterPercent: Integer
      read GetReconnectJitterPercent write SetReconnectJitterPercent;
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
    FReconnectJitterPercent: Integer;
    FOnDisconnect: TMQTTDisconnectHandler;
    FOnError: TMQTTErrorHandler;
    FOnConnect: TMQTTConnectHandler;
    FOnPacketReceived: TMQTTPacketHandler;
    FOnPacketSent: TMQTTPacketHandler;
    FOnSubscribeAck: TMQTTSubscribeAckHandler;
    FLogger: IMQTTLogger;
    FShutdown: Boolean;
    FStopEvent: TEvent;           // wakes the pinger / reconnect back-off on Disconnect
    FReceiverThreadID: TThreadID;
    FPingSentAt: TDateTime;       // 0 = no PINGREQ outstanding
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
    procedure CompletePending(PacketID: Word; Success: Boolean; const Why: string);
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
    procedure SetReconnectInitialDelayMs(Value: Integer);
    function GetReconnectInitialDelayMs: Integer;
    procedure SetReconnectMaxDelayMs(Value: Integer);
    function GetReconnectMaxDelayMs: Integer;
    procedure SetReconnectJitterPercent(Value: Integer);
    function GetReconnectJitterPercent: Integer;
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
  // Reads only start once CheckForDataOnSource saw data, so this is the
  // budget for the rest of a packet to arrive, not a polling interval.
  // A timeout mid-packet leaves the stream desynchronised and is fatal.
  FIndy.ReadTimeout := 30000;
  FSSLHandler := nil;
  FSSLOptions.SetDefaults;
  FSubscriptions := TDictionary<string, TSubscriptionInfo>.Create;
  FPendingPublish := TDictionary<Word, TPendingPublish>.Create;
  FPendingQoS2Inbound := TDictionary<Word, TPendingQoS2Inbound>.Create;
  FLock := TCriticalSection.Create;
  FWriteLock := TCriticalSection.Create;
  FStopEvent := TEvent.Create(nil, True, False, '');
  FState := Disconnected;
  FNextPacketID := 1;
  FAutoReconnect := False;
  FReconnectDelayMs := 1000;
  FMaxReconnectDelayMs := 30000;
  FReconnectJitterPercent := 25;
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
  FStopEvent.Free;
  FIndy.Free;
  inherited Destroy;
end;

function TMQTTClient.GetNextPacketID: Word;
begin
  FLock.Enter;
  try
    // ponytail: linear skip; loops forever only with 65535 publishes in flight
    repeat
      Result := FNextPacketID;
      Inc(FNextPacketID);
      if FNextPacketID = 0 then
        FNextPacketID := 1;
    until not FPendingPublish.ContainsKey(Result);
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

procedure TMQTTClient.SetReconnectInitialDelayMs(Value: Integer);
begin
  if Value < 1 then
    raise EMQTTException.Create('ReconnectInitialDelayMs must be >= 1');
  FLock.Enter;
  try
    FReconnectDelayMs := Value;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.GetReconnectInitialDelayMs: Integer;
begin
  FLock.Enter;
  try
    Result := FReconnectDelayMs;
  finally
    FLock.Leave;
  end;
end;

procedure TMQTTClient.SetReconnectMaxDelayMs(Value: Integer);
begin
  if Value < 1 then
    raise EMQTTException.Create('ReconnectMaxDelayMs must be >= 1');
  FLock.Enter;
  try
    FMaxReconnectDelayMs := Value;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.GetReconnectMaxDelayMs: Integer;
begin
  FLock.Enter;
  try
    Result := FMaxReconnectDelayMs;
  finally
    FLock.Leave;
  end;
end;

procedure TMQTTClient.SetReconnectJitterPercent(Value: Integer);
begin
  if (Value < 0) or (Value > 100) then
    raise EMQTTException.Create('ReconnectJitterPercent must be in 0..100');
  FLock.Enter;
  try
    FReconnectJitterPercent := Value;
  finally
    FLock.Leave;
  end;
end;

function TMQTTClient.GetReconnectJitterPercent: Integer;
begin
  FLock.Enter;
  try
    Result := FReconnectJitterPercent;
  finally
    FLock.Leave;
  end;
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
  FStopEvent.ResetEvent;
  FPingSentAt := 0;
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

  // Outside the try: the connection is up and the threads are running, so an
  // exception from user code must not tear the socket down under them.
  if Assigned(FOnConnect) then
    FOnConnect(TMQTTReasonCode(ReasonCode), SessionPresent);
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
  FStopEvent.SetEvent;

  // FState may already be Disconnected because the receiver lost the
  // connection: the threads can still be running, so never exit early here.
  if FState <> Disconnected then
  begin
    SnapshotLogger.Info('Disconnecting from %s:%d', [FHost, FPort]);
    if FIndy.Connected then
    begin
      try
        Packet := TMQTTProtocol.BuildDisconnect(Ord(FOptions.Version));
        SendPacket(Packet);
      except
        // Ignore send errors during disconnect
      end;
    end;
  end;

  FState := Disconnected;

  try
    if FIndy.Connected then
      FIndy.Disconnect;
  except
    // socket already gone
  end;

  // Join the threads before anything they use can be freed (Destroy calls
  // this). Not when called from a message handler: that IS the receiver,
  // and it exits on its own once the handler returns and sees FShutdown.
  if Assigned(FPinger) then
  begin
    try
      FPinger.Wait;
    except
      // task exceptions are already reported through OnError
    end;
    FPinger := nil;
  end;

  if Assigned(FReceiver) then
  begin
    if TThread.CurrentThread.ThreadID <> FReceiverThreadID then
    begin
      try
        FReceiver.Wait;
      except
        // task exceptions are already reported through OnError
      end;
    end;
    FReceiver := nil;
  end;

  // A later plain Connect must not inherit the TLS handler of this session
  CleanupSSL;
  FSSLOptions.Enabled := False;

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
    Multiplier := Multiplier * 128;
    if ((Digit and 128) <> 0) and (Multiplier > 128 * 128 * 128) then
      raise EMQTTProtocolException.Create('Remaining length too large');
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
end;

procedure TMQTTClient.ReceiverLoop;
var
  Packet: TBytes;
begin
  FReceiverThreadID := TThread.CurrentThread.ThreadID;
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
      on E: EIdConnClosedGracefully do
      begin
        if FShutdown then
          Break;
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
        if FShutdown then
          Break; // our own Disconnect closed the socket
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
  FReceiverThreadID := 0;
end;

procedure TMQTTClient.PingerLoop;
var
  KeepAliveInterval: Double;
  Packet: TBytes;
begin
  KeepAliveInterval := FOptions.KeepAliveSec * 0.75; // Send ping at 75% of keep-alive

  while not FShutdown and (FState = Connected) do
  begin
    FStopEvent.WaitFor(1000);

    if FShutdown or (FState <> Connected) then
      Break;

    // No PINGRESP within a full keep-alive: the connection is half-open.
    // Closing the socket makes the receiver run its normal lost-connection path.
    if (FPingSentAt <> 0) and (SecondsBetween(Now, FPingSentAt) >= FOptions.KeepAliveSec) then
    begin
      SnapshotLogger.Warning('No PINGRESP within %d s, closing connection', [FOptions.KeepAliveSec]);
      FPingSentAt := 0;
      try
        FIndy.Disconnect;
      except
        // already closed
      end;
      Break;
    end;

    // The client must *send* something within the keep-alive, so only
    // outbound traffic (SendPacket) refreshes FLastActivity.
    if (FPingSentAt = 0) and (SecondsBetween(Now, FLastActivity) >= KeepAliveInterval) then
    begin
      try
        FPingSentAt := Now;
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
begin
  if not TMQTTProtocol.ParsePublish(Packet, Ord(FOptions.Version), Topic, Payload, QoS, Retain, Dup, PacketID) then
    Exit;

  case QoS of
    atMostOnce:
      begin
        // No ACK needed for QoS 0
        DispatchMessageEx(Topic, Payload, Dup, QoS, 0);
        DispatchMessageManualAck(Topic, Payload, Dup, QoS, 0);
      end;

    atLeastOnce:
      begin
        // Dispatch first, then ACK. This preserves the "at-least-once to
        // application" guarantee: if the process dies between receive and
        // ACK, the broker redelivers. Manual-ack handlers can veto the ACK
        // (DispatchMessageManualAck returns True when there are none).
        DispatchMessageEx(Topic, Payload, Dup, QoS, PacketID);
        if DispatchMessageManualAck(Topic, Payload, Dup, QoS, PacketID) then
          SendAck(QoS, PacketID);
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
begin
  if not TMQTTProtocol.ParsePubAck(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;
  CompletePending(PacketID, ReasonCode < $80, Format('PUBACK reason code $%.2x', [ReasonCode]));
end;

procedure TMQTTClient.CompletePending(PacketID: Word; Success: Boolean; const Why: string);
var
  Pending: TPendingPublish;
begin
  FLock.Enter;
  try
    if not FPendingPublish.TryGetValue(PacketID, Pending) then
      Exit;
    // A PublishSync waiter reads the outcome from whether its entry is
    // still there, so a failed sync entry stays (psFailed) and the waiter
    // removes it. Signalling under FLock means the waiter cannot free the
    // event in between.
    if Success or not Assigned(Pending.AckEvent) then
      FPendingPublish.Remove(PacketID)
    else
    begin
      Pending.State := psFailed;
      FPendingPublish[PacketID] := Pending;
    end;
    if Assigned(Pending.AckEvent) then
      Pending.AckEvent.SetEvent;
  finally
    FLock.Leave;
  end;
  if not Success then
    DoError(Format('Publish failed: PacketID=%d topic="%s" (%s)', [PacketID, Pending.Topic, Why]));
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

  // MQTT 5: a PUBREC >= $80 ends the QoS 2 flow, no PUBREL follows
  if ReasonCode >= $80 then
  begin
    CompletePending(PacketID, False, Format('PUBREC reason code $%.2x', [ReasonCode]));
    Exit;
  end;

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
  ShouldDispatch, ShouldAck: Boolean;
begin
  if not TMQTTProtocol.ParsePubRel(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;

  FLock.Enter;
  try
    ShouldDispatch := FPendingQoS2Inbound.TryGetValue(PacketID, PendingMsg);
  finally
    FLock.Leave;
  end;

  // Dispatch outside lock
  ShouldAck := True; // Default: send PUBCOMP
  if ShouldDispatch then
  begin
    DispatchMessageEx(PendingMsg.Topic, PendingMsg.Payload, PendingMsg.Dup, exactlyOnce, PacketID);
    ShouldAck := DispatchMessageManualAck(PendingMsg.Topic, PendingMsg.Payload, PendingMsg.Dup, exactlyOnce, PacketID);
  end;

  // Send PUBCOMP only if acknowledged. A declined message stays stored so
  // the PUBREL the broker resends can dispatch it again.
  if ShouldAck then
  begin
    FLock.Enter;
    try
      FPendingQoS2Inbound.Remove(PacketID);
    finally
      FLock.Leave;
    end;
    CompPacket := TMQTTProtocol.BuildPubComp(Ord(FOptions.Version), PacketID);
    SendPacket(CompPacket);
  end;
end;

procedure TMQTTClient.HandlePubComp(const Packet: TBytes);
var
  PacketID: Word;
  ReasonCode: Byte;
begin
  if not TMQTTProtocol.ParsePubComp(Packet, Ord(FOptions.Version), PacketID, ReasonCode) then
    Exit;
  CompletePending(PacketID, ReasonCode < $80, Format('PUBCOMP reason code $%.2x', [ReasonCode]));
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
  FPingSentAt := 0;
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
    // Every matching filter gets the message: "a/b" and "a/#" both fire.
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
  BaseDelay, EffectiveDelay, JitterMs, JitterPct, MaxDelay: Integer;
begin
  if FShutdown then
    Exit;

  FState := Reconnecting;
  BaseDelay := GetReconnectInitialDelayMs;
  MaxDelay := GetReconnectMaxDelayMs;
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
    // Apply random jitter around BaseDelay to avoid thundering-herd reconnects
    // when many clients lose the broker simultaneously. Jitter range is
    // +/- JitterPct% of BaseDelay; with JitterPct=0 the effective delay equals
    // BaseDelay (jitter disabled).
    JitterPct := GetReconnectJitterPercent;
    if JitterPct > 0 then
    begin
      JitterMs := (BaseDelay * JitterPct) div 100;
      if JitterMs < 1 then
        JitterMs := 1;
      EffectiveDelay := BaseDelay + (Random(JitterMs * 2 + 1) - JitterMs);
      if EffectiveDelay < 50 then
        EffectiveDelay := 50;
    end
    else
      EffectiveDelay := BaseDelay;

    SnapshotLogger.Info('Reconnect attempt in %d ms (base=%d, jitter=%d%%)',
      [EffectiveDelay, BaseDelay, JitterPct]);
    FStopEvent.WaitFor(EffectiveDelay); // Disconnect wakes us immediately

    if FShutdown then
      Break;

    try
      // Hold the write lock while the socket and TLS handler are swapped, so
      // a publisher that is already inside SendPacket cannot write to a freed
      // handler or ahead of CONNECT. The lock is re-entrant.
      var Packet: TBytes;
      FWriteLock.Enter;
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
        Packet := TMQTTProtocol.BuildConnect(FOptions);
        SendPacket(Packet);
      finally
        FWriteLock.Leave;
      end;

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
          FPingSentAt := 0;
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

    // Exponential backoff on BaseDelay; jitter is applied per-iteration above.
    BaseDelay := Min(BaseDelay * 2, MaxDelay);
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
        if (Pair.Value.State <> psFailed) and (SecondsBetween(Now, Pair.Value.Timestamp) > 5) then
          ToRetry.Add(Pair.Value);
      end;
    finally
      FLock.Leave;
    end;

    for Pending in ToRetry do
    begin
      if Pending.RetryCount >= 3 then
      begin
        SnapshotLogger.Warning(
          'Pending publish dropped after 3 retries: PacketID=%d topic="%s" qos=%d',
          [Pending.PacketID, Pending.Topic, Ord(Pending.QoS)]);
        // Also wakes a PublishSync caller so it returns False right away
        CompletePending(Pending.PacketID, False, 'no ACK after 3 retries');
        Continue;
      end;

      // The ACK may have arrived since the snapshot: don't resend a DUP then
      FLock.Enter;
      try
        if not FPendingPublish.ContainsKey(Pending.PacketID) then
          Continue;
      finally
        FLock.Leave;
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

  // Store for QoS 1/2 BEFORE sending: on a fast broker the ACK can be
  // processed by the receiver before an Add placed after SendPacket.
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

  try
    SendPacket(Packet);
  except
    if QoS > atMostOnce then
    begin
      FLock.Enter;
      try
        FPendingPublish.Remove(PacketID);
      finally
        FLock.Leave;
      end;
    end;
    raise;
  end;
end;

function TMQTTClient.PublishSync(const Topic: string; const Payload: TBytes; QoS: TMQTTQoS;
  Retain: Boolean; TimeoutMs: Cardinal): Boolean;
var
  PacketID: Word;
  Packet: TBytes;
  Pending: TPendingPublish;
  PerCallEvent: TEvent;
begin
  if not IsConnected then
    raise EMQTTException.Create('Not connected');

  if not TMQTTProtocol.IsValidTopicName(Topic) then
    raise EMQTTException.Create('Invalid topic name');

  if QoS = atMostOnce then
  begin
    Publish(Topic, Payload, QoS, Retain);
    Result := True;
    Exit;
  end;

  // Handlers run on the receiver thread, the only one that can process the
  // ACK we would be waiting for: waiting there always times out.
  if TThread.CurrentThread.ThreadID = FReceiverThreadID then
    raise EMQTTException.Create('PublishSync cannot be called from a message handler; use Publish');

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

    try
      SendPacket(Packet);
      PerCallEvent.WaitFor(TimeoutMs);
    finally
      // Success removed the entry; timeout or failure left it (see
      // CompletePending). Removing it under FLock guarantees nobody can
      // signal the event after it is freed below.
      FLock.Enter;
      try
        Result := not FPendingPublish.ContainsKey(PacketID);
        FPendingPublish.Remove(PacketID);
      finally
        FLock.Leave;
      end;
    end;
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
