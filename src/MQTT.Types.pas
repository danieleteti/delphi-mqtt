unit MQTT.Types;

interface

uses
  System.SysUtils,
  System.Generics.Collections;

const
  MQTT_LIB_VERSION = '1.0.0';
  MQTT_LIB_VERSION_MAJOR = 1;
  MQTT_LIB_VERSION_MINOR = 0;
  MQTT_LIB_VERSION_PATCH = 0;

type
  EMQTTException = class(Exception);
  EMQTTProtocolException = class(EMQTTException);
  EMQTTConnectionException = class(EMQTTException);
  EMQTTTimeoutException = class(EMQTTException);

  TMQTTPacketType = (
    ptReserved = 0,
    ptConnect = 1,
    ptConnAck = 2,
    ptPublish = 3,
    ptPubAck = 4,
    ptPubRec = 5,
    ptPubRel = 6,
    ptPubComp = 7,
    ptSubscribe = 8,
    ptSubAck = 9,
    ptUnsubscribe = 10,
    ptUnsubAck = 11,
    ptPingReq = 12,
    ptPingResp = 13,
    ptDisconnect = 14,
    ptAuth = 15
  );

  TMQTTQoS = (
    atMostOnce = 0,
    atLeastOnce = 1,
    exactlyOnce = 2
  );

  TMQTTVersion = (
    MQTT311 = 4,
    MQTT5 = 5
  );

  TMQTTConnectionState = (
    Disconnected,
    Connecting,
    Connected,
    Reconnecting
  );

  TMQTTSSLMethod = (
    sslAuto,      // Auto-negotiate best available
    sslTLS1,      // TLS 1.0
    sslTLS1_1,    // TLS 1.1
    sslTLS1_2,    // TLS 1.2 (recommended minimum)
    sslTLS1_3     // TLS 1.3 (if supported by OpenSSL)
  );

  TMQTTSSLVerifyMode = (
    sslVerifyNone,      // No certificate verification (insecure, for testing)
    sslVerifyPeer       // Verify server certificate (recommended)
  );

  TMQTTSSLOptions = record
    Enabled: Boolean;
    CertFile: string;         // Client certificate file (PEM format)
    KeyFile: string;          // Client private key file (PEM format)
    RootCertFile: string;     // CA root certificate file (PEM format)
    KeyPassword: string;      // Password for private key (if encrypted)
    Method: TMQTTSSLMethod;
    VerifyMode: TMQTTSSLVerifyMode;
    VerifyDepth: Integer;     // Certificate chain verification depth
    procedure SetDefaults;
  end;

  TMQTTReasonCode = (
    rcSuccess = 0,
    rcGrantedQoS1 = 1,
    rcGrantedQoS2 = 2,
    rcDisconnectWithWill = 4,
    rcNoMatchingSubscribers = 16,
    rcNoSubscriptionExisted = 17,
    rcContinueAuthentication = 24,
    rcReAuthenticate = 25,
    rcUnspecifiedError = 128,
    rcMalformedPacket = 129,
    rcProtocolError = 130,
    rcImplementationSpecificError = 131,
    rcUnsupportedProtocolVersion = 132,
    rcClientIdentifierNotValid = 133,
    rcBadUserNameOrPassword = 134,
    rcNotAuthorized = 135,
    rcServerUnavailable = 136,
    rcServerBusy = 137,
    rcBanned = 138,
    rcServerShuttingDown = 139,
    rcBadAuthenticationMethod = 140,
    rcKeepAliveTimeout = 141,
    rcSessionTakenOver = 142,
    rcTopicFilterInvalid = 143,
    rcTopicNameInvalid = 144,
    rcPacketIdentifierInUse = 145,
    rcPacketIdentifierNotFound = 146,
    rcReceiveMaximumExceeded = 147,
    rcTopicAliasInvalid = 148,
    rcPacketTooLarge = 149,
    rcMessageRateTooHigh = 150,
    rcQuotaExceeded = 151,
    rcAdministrativeAction = 152,
    rcPayloadFormatInvalid = 153,
    rcRetainNotSupported = 154,
    rcQoSNotSupported = 155,
    rcUseAnotherServer = 156,
    rcServerMoved = 157,
    rcSharedSubscriptionsNotSupported = 158,
    rcConnectionRateExceeded = 159,
    rcMaximumConnectTime = 160,
    rcSubscriptionIdentifiersNotSupported = 161,
    rcWildcardSubscriptionsNotSupported = 162
  );

  TMQTTPropertyID = (
    propPayloadFormatIndicator = 1,
    propMessageExpiryInterval = 2,
    propContentType = 3,
    propResponseTopic = 8,
    propCorrelationData = 9,
    propSubscriptionIdentifier = 11,
    propSessionExpiryInterval = 17,
    propAssignedClientIdentifier = 18,
    propServerKeepAlive = 19,
    propAuthenticationMethod = 21,
    propAuthenticationData = 22,
    propRequestProblemInformation = 23,
    propWillDelayInterval = 24,
    propRequestResponseInformation = 25,
    propResponseInformation = 26,
    propServerReference = 28,
    propReasonString = 31,
    propReceiveMaximum = 33,
    propTopicAliasMaximum = 34,
    propTopicAlias = 35,
    propMaximumQoS = 36,
    propRetainAvailable = 37,
    propUserProperty = 38,
    propMaximumPacketSize = 39,
    propWildcardSubscriptionAvailable = 40,
    propSubscriptionIdentifierAvailable = 41,
    propSharedSubscriptionAvailable = 42
  );

  TMQTTWillOptions = record
    Enabled: Boolean;
    Topic: string;
    Payload: TBytes;
    QoS: TMQTTQoS;
    Retain: Boolean;
    DelayInterval: Cardinal;
    PayloadFormatIndicator: Byte;
    MessageExpiryInterval: Cardinal;
    ContentType: string;
    ResponseTopic: string;
    CorrelationData: TBytes;
    procedure Clear;
  end;

  TMQTTUserProperty = record
    Key: string;
    Value: string;
  end;
  TMQTTUserProperties = TArray<TMQTTUserProperty>;

  TMQTTConnectOptions = record
    ClientID: string;
    KeepAliveSec: Word;
    CleanStart: Boolean;
    Version: TMQTTVersion;
    Username: string;
    Password: string;
    Will: TMQTTWillOptions;
    SessionExpiryInterval: Cardinal;
    ReceiveMaximum: Word;
    MaxPacketSize: Cardinal;
    TopicAliasMaximum: Word;
    RequestResponseInfo: Boolean;
    RequestProblemInfo: Boolean;
    procedure SetDefaults;
  end;

  TMQTTPublishOptions = record
    Topic: string;
    Payload: TBytes;
    QoS: TMQTTQoS;
    Retain: Boolean;
    Dup: Boolean;
    PayloadFormatIndicator: Byte;
    MessageExpiryInterval: Cardinal;
    TopicAlias: Word;
    ResponseTopic: string;
    CorrelationData: TBytes;
    ContentType: string;
    procedure Clear;
  end;

  TMQTTMessage = record
    Topic: string;
    Payload: TBytes;
    QoS: TMQTTQoS;
    Retain: Boolean;
    PacketID: Word;
    function PayloadAsString: string;
    class function Create(const ATopic: string; const APayload: TBytes; AQoS: TMQTTQoS = atMostOnce): TMQTTMessage; static;
  end;

  TMQTTSubscription = record
    Topic: string;
    QoS: TMQTTQoS;
    NoLocal: Boolean;
    RetainAsPublished: Boolean;
    RetainHandling: Byte;
  end;
  TMQTTSubscriptions = TArray<TMQTTSubscription>;

  // Anonymous method handlers (reference to procedure)
  TMQTTMessageHandler = reference to procedure(const Msg: TMQTTMessage);
  TMQTTDisconnectHandler = reference to procedure(ReasonCode: TMQTTReasonCode; const ReasonString: string);
  TMQTTErrorHandler = reference to procedure(const ErrorMsg: string);
  TMQTTConnectHandler = reference to procedure(ReasonCode: TMQTTReasonCode);
  TMQTTPublishAckHandler = reference to procedure(PacketID: Word; ReasonCode: TMQTTReasonCode);

  // Method pointer handlers (of object)
  TMQTTMessageEvent = procedure(const Topic: string; const Payload: TBytes) of object;
  TMQTTDisconnectEvent = procedure(ReasonCode: TMQTTReasonCode; const ReasonString: string) of object;
  TMQTTErrorEvent = procedure(const ErrorMsg: string) of object;
  TMQTTConnectEvent = procedure(ReasonCode: TMQTTReasonCode) of object;

  // Extended handler with Dup flag and QoS info
  TMQTTExtendedHandler = reference to procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS);
  TMQTTExtendedEvent = procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; QoS: TMQTTQoS) of object;

  // Manual ACK handler - set Ack := True to acknowledge, False (default) to reject/redeliver
  // Dup = True means this is a redelivered message (you may have seen it before)
  TMQTTManualAckHandler = reference to procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; var Ack: Boolean);
  TMQTTManualAckEvent = procedure(const Topic: string; const Payload: TBytes; Dup: Boolean; var Ack: Boolean) of object;

  // Packet monitoring (P3) - fired on every packet sent/received
  // Direction: True = received, False = sent
  TMQTTPacketHandler = reference to procedure(PacketType: TMQTTPacketType; const RawPacket: TBytes);
  TMQTTPacketEvent = procedure(PacketType: TMQTTPacketType; const RawPacket: TBytes) of object;

  // Subscription acknowledgment (P4) - fired when broker confirms SUBSCRIBE
  // GrantedQoS contains the QoS actually granted by the broker per topic filter
  // (0x80 / 128 = failure / not authorized)
  TMQTTSubscribeAckHandler = reference to procedure(PacketID: Word; const GrantedQoS: TArray<Byte>);
  TMQTTSubscribeAckEvent = procedure(PacketID: Word; const GrantedQoS: TArray<Byte>) of object;

implementation

{ TMQTTSSLOptions }

procedure TMQTTSSLOptions.SetDefaults;
begin
  Enabled := False;
  CertFile := '';
  KeyFile := '';
  RootCertFile := '';
  KeyPassword := '';
  Method := sslTLS1_2;
  VerifyMode := sslVerifyPeer;
  VerifyDepth := 9;
end;

{ TMQTTWillOptions }

procedure TMQTTWillOptions.Clear;
begin
  Enabled := False;
  Topic := '';
  SetLength(Payload, 0);
  QoS := atMostOnce;
  Retain := False;
  DelayInterval := 0;
  PayloadFormatIndicator := 0;
  MessageExpiryInterval := 0;
  ContentType := '';
  ResponseTopic := '';
  SetLength(CorrelationData, 0);
end;

{ TMQTTConnectOptions }

procedure TMQTTConnectOptions.SetDefaults;
begin
  ClientID := '';
  KeepAliveSec := 60;
  CleanStart := True;
  Version := MQTT5;
  Username := '';
  Password := '';
  Will.Clear;
  SessionExpiryInterval := 0;
  ReceiveMaximum := 65535;
  MaxPacketSize := 0;
  TopicAliasMaximum := 0;
  RequestResponseInfo := False;
  RequestProblemInfo := True;
end;

{ TMQTTPublishOptions }

procedure TMQTTPublishOptions.Clear;
begin
  Topic := '';
  SetLength(Payload, 0);
  QoS := atMostOnce;
  Retain := False;
  Dup := False;
  PayloadFormatIndicator := 0;
  MessageExpiryInterval := 0;
  TopicAlias := 0;
  ResponseTopic := '';
  SetLength(CorrelationData, 0);
  ContentType := '';
end;

{ TMQTTMessage }

class function TMQTTMessage.Create(const ATopic: string; const APayload: TBytes; AQoS: TMQTTQoS): TMQTTMessage;
begin
  Result.Topic := ATopic;
  Result.Payload := APayload;
  Result.QoS := AQoS;
  Result.Retain := False;
  Result.PacketID := 0;
end;

function TMQTTMessage.PayloadAsString: string;
begin
  if Length(Payload) > 0 then
    Result := TEncoding.UTF8.GetString(Payload)
  else
    Result := '';
end;

end.
