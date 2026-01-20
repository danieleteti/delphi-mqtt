unit MQTT.Protocol;

interface

uses
  System.SysUtils,
  System.Classes,
  MQTT.Types;

type
  TMQTTProtocol = class
  public
    // Encoding/Decoding
    class function EncodeLen(Value: Integer): TBytes;
    class function DecodeLen(Stream: TStream): Integer; overload;
    class function DecodeLen(const Data: TBytes; var Offset: Integer): Integer; overload;
    class function EncodeStr(const S: string): TBytes;
    class function DecodeStr(Stream: TStream): string; overload;
    class function DecodeStr(const Data: TBytes; var Offset: Integer): string; overload;
    class function EncodeBinary(const Data: TBytes): TBytes;
    class function DecodeBinary(Stream: TStream): TBytes; overload;
    class function DecodeBinary(const Data: TBytes; var Offset: Integer): TBytes; overload;

    // Packet Builders
    class function BuildConnect(const Options: TMQTTConnectOptions): TBytes; overload;
    class function BuildConnect(Version: Byte; const ClientID: string; KeepAlive: Word; Clean: Boolean; const Will: TMQTTWillOptions): TBytes; overload;
    class function BuildPublish(Version: Byte; const Topic: string; const Payload: TBytes; QoS: TMQTTQoS; Retain, Dup: Boolean; PacketID: Word): TBytes;
    class function BuildSubscribe(Version: Byte; PacketID: Word; const Topics: TMQTTSubscriptions): TBytes; overload;
    class function BuildSubscribe(Version: Byte; PacketID: Word; const Topic: string; QoS: TMQTTQoS): TBytes; overload;
    class function BuildUnsubscribe(Version: Byte; PacketID: Word; const Topics: TArray<string>): TBytes;
    class function BuildPubAck(Version: Byte; PacketID: Word; ReasonCode: Byte = 0): TBytes;
    class function BuildPubRec(Version: Byte; PacketID: Word; ReasonCode: Byte = 0): TBytes;
    class function BuildPubRel(Version: Byte; PacketID: Word; ReasonCode: Byte = 0): TBytes;
    class function BuildPubComp(Version: Byte; PacketID: Word; ReasonCode: Byte = 0): TBytes;
    class function BuildPingReq: TBytes;
    class function BuildPingResp: TBytes;
    class function BuildDisconnect(Version: Byte; ReasonCode: Byte = 0): TBytes;

    // Packet Parsing
    class function ParseConnAck(const Packet: TBytes; Version: Byte; out SessionPresent: Boolean; out ReasonCode: Byte): Boolean;
    class function ParsePublish(const Packet: TBytes; Version: Byte; out Topic: string; out Payload: TBytes; out QoS: TMQTTQoS; out Retain, Dup: Boolean; out PacketID: Word): Boolean;
    class function ParseSubAck(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCodes: TArray<Byte>): Boolean;
    class function ParseUnsubAck(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCodes: TArray<Byte>): Boolean;
    class function ParsePubAck(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCode: Byte): Boolean;
    class function ParsePubRec(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCode: Byte): Boolean;
    class function ParsePubRel(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCode: Byte): Boolean;
    class function ParsePubComp(const Packet: TBytes; Version: Byte; out PacketID: Word; out ReasonCode: Byte): Boolean;

    // Wildcard Matching
    class function TopicMatchesFilter(const Topic, Filter: string): Boolean;
    class function IsValidTopicFilter(const Filter: string): Boolean;
    class function IsValidTopicName(const Topic: string): Boolean;

    // Utilities
    class function GetPacketType(const Packet: TBytes): TMQTTPacketType;
    class function GetPacketID(const Packet: TBytes): Word;
  end;

implementation

uses
  System.StrUtils;

{ TMQTTProtocol }

class function TMQTTProtocol.EncodeLen(Value: Integer): TBytes;
var
  Digit: Byte;
begin
  SetLength(Result, 0);
  repeat
    Digit := Value mod 128;
    Value := Value div 128;
    if Value > 0 then
      Digit := Digit or 128;
    SetLength(Result, Length(Result) + 1);
    Result[High(Result)] := Digit;
  until Value = 0;
end;

class function TMQTTProtocol.DecodeLen(Stream: TStream): Integer;
var
  Digit: Byte;
  Multiplier: Integer;
begin
  Result := 0;
  Multiplier := 1;
  repeat
    if Stream.Read(Digit, 1) <> 1 then
      raise EMQTTProtocolException.Create('Unexpected end of stream decoding length');
    Result := Result + (Digit and 127) * Multiplier;
    if Multiplier > 128 * 128 * 128 then
      raise EMQTTProtocolException.Create('Malformed remaining length');
    Multiplier := Multiplier * 128;
  until (Digit and 128) = 0;
end;

class function TMQTTProtocol.DecodeLen(const Data: TBytes; var Offset: Integer): Integer;
var
  Digit: Byte;
  Multiplier: Integer;
begin
  Result := 0;
  Multiplier := 1;
  repeat
    if Offset >= Length(Data) then
      raise EMQTTProtocolException.Create('Unexpected end of data decoding length');
    Digit := Data[Offset];
    Inc(Offset);
    Result := Result + (Digit and 127) * Multiplier;
    if Multiplier > 128 * 128 * 128 then
      raise EMQTTProtocolException.Create('Malformed remaining length');
    Multiplier := Multiplier * 128;
  until (Digit and 128) = 0;
end;

class function TMQTTProtocol.EncodeStr(const S: string): TBytes;
var
  B: TBytes;
  L: Word;
begin
  B := TEncoding.UTF8.GetBytes(S);
  L := Length(B);
  SetLength(Result, L + 2);
  Result[0] := Hi(L);
  Result[1] := Lo(L);
  if L > 0 then
    Move(B[0], Result[2], L);
end;

class function TMQTTProtocol.DecodeStr(Stream: TStream): string;
var
  LBytes: array[0..1] of Byte;
  L: Word;
  B: TBytes;
begin
  if Stream.Read(LBytes, 2) <> 2 then
    raise EMQTTProtocolException.Create('Unexpected end of stream decoding string length');
  L := (LBytes[0] shl 8) or LBytes[1];
  if L = 0 then
    Exit('');
  SetLength(B, L);
  if Stream.Read(B[0], L) <> L then
    raise EMQTTProtocolException.Create('Unexpected end of stream decoding string');
  Result := TEncoding.UTF8.GetString(B);
end;

class function TMQTTProtocol.DecodeStr(const Data: TBytes; var Offset: Integer): string;
var
  L: Word;
  B: TBytes;
begin
  if Offset + 2 > Length(Data) then
    raise EMQTTProtocolException.Create('Unexpected end of data decoding string length');
  L := (Data[Offset] shl 8) or Data[Offset + 1];
  Inc(Offset, 2);
  if L = 0 then
    Exit('');
  if Offset + L > Length(Data) then
    raise EMQTTProtocolException.Create('Unexpected end of data decoding string');
  SetLength(B, L);
  Move(Data[Offset], B[0], L);
  Inc(Offset, L);
  Result := TEncoding.UTF8.GetString(B);
end;

class function TMQTTProtocol.EncodeBinary(const Data: TBytes): TBytes;
var
  L: Word;
begin
  L := Length(Data);
  SetLength(Result, L + 2);
  Result[0] := Hi(L);
  Result[1] := Lo(L);
  if L > 0 then
    Move(Data[0], Result[2], L);
end;

class function TMQTTProtocol.DecodeBinary(Stream: TStream): TBytes;
var
  LBytes: array[0..1] of Byte;
  L: Word;
begin
  if Stream.Read(LBytes, 2) <> 2 then
    raise EMQTTProtocolException.Create('Unexpected end of stream decoding binary length');
  L := (LBytes[0] shl 8) or LBytes[1];
  SetLength(Result, L);
  if L > 0 then
    if Stream.Read(Result[0], L) <> L then
      raise EMQTTProtocolException.Create('Unexpected end of stream decoding binary');
end;

class function TMQTTProtocol.DecodeBinary(const Data: TBytes; var Offset: Integer): TBytes;
var
  L: Word;
begin
  if Offset + 2 > Length(Data) then
    raise EMQTTProtocolException.Create('Unexpected end of data decoding binary length');
  L := (Data[Offset] shl 8) or Data[Offset + 1];
  Inc(Offset, 2);
  SetLength(Result, L);
  if L > 0 then
  begin
    if Offset + L > Length(Data) then
      raise EMQTTProtocolException.Create('Unexpected end of data decoding binary');
    Move(Data[Offset], Result[0], L);
    Inc(Offset, L);
  end;
end;

class function TMQTTProtocol.BuildConnect(const Options: TMQTTConnectOptions): TBytes;
var
  VH, PL, RL, Props: TBytes;
  Flags: Byte;
  Version: Byte;
  ClientID: string;
begin
  Version := Ord(Options.Version);
  ClientID := Options.ClientID;
  if ClientID = '' then
    ClientID := 'DelphiMQTT_' + IntToStr(Random(100000));

  // Variable Header
  VH := EncodeStr('MQTT');
  SetLength(VH, Length(VH) + 1);
  VH[High(VH)] := Version;

  // Connect Flags
  Flags := 0;
  if Options.CleanStart then
    Flags := Flags or $02;
  if Options.Will.Enabled then
  begin
    Flags := Flags or $04;
    Flags := Flags or (Ord(Options.Will.QoS) shl 3);
    if Options.Will.Retain then
      Flags := Flags or $20;
  end;
  if Options.Password <> '' then
    Flags := Flags or $40;
  if Options.Username <> '' then
    Flags := Flags or $80;

  VH := VH + [Flags];
  VH := VH + [Hi(Options.KeepAliveSec), Lo(Options.KeepAliveSec)];

  // Properties (MQTT 5.0)
  if Version = 5 then
  begin
    SetLength(Props, 0);
    if Options.SessionExpiryInterval > 0 then
      Props := Props + [Byte(propSessionExpiryInterval),
        Byte(Options.SessionExpiryInterval shr 24),
        Byte(Options.SessionExpiryInterval shr 16),
        Byte(Options.SessionExpiryInterval shr 8),
        Byte(Options.SessionExpiryInterval)];
    if Options.ReceiveMaximum < 65535 then
      Props := Props + [Byte(propReceiveMaximum),
        Hi(Options.ReceiveMaximum), Lo(Options.ReceiveMaximum)];
    if Options.MaxPacketSize > 0 then
      Props := Props + [Byte(propMaximumPacketSize),
        Byte(Options.MaxPacketSize shr 24),
        Byte(Options.MaxPacketSize shr 16),
        Byte(Options.MaxPacketSize shr 8),
        Byte(Options.MaxPacketSize)];
    if Options.TopicAliasMaximum > 0 then
      Props := Props + [Byte(propTopicAliasMaximum),
        Hi(Options.TopicAliasMaximum), Lo(Options.TopicAliasMaximum)];
    if Options.RequestResponseInfo then
      Props := Props + [Byte(propRequestResponseInformation), 1];
    if not Options.RequestProblemInfo then
      Props := Props + [Byte(propRequestProblemInformation), 0];

    VH := VH + EncodeLen(Length(Props)) + Props;
  end;

  // Payload
  PL := EncodeStr(ClientID);

  // Will Properties and Will Message (if enabled)
  if Options.Will.Enabled then
  begin
    if Version = 5 then
    begin
      SetLength(Props, 0);
      if Options.Will.DelayInterval > 0 then
        Props := Props + [Byte(propWillDelayInterval),
          Byte(Options.Will.DelayInterval shr 24),
          Byte(Options.Will.DelayInterval shr 16),
          Byte(Options.Will.DelayInterval shr 8),
          Byte(Options.Will.DelayInterval)];
      if Options.Will.PayloadFormatIndicator > 0 then
        Props := Props + [Byte(propPayloadFormatIndicator),
          Options.Will.PayloadFormatIndicator];
      if Options.Will.MessageExpiryInterval > 0 then
        Props := Props + [Byte(propMessageExpiryInterval),
          Byte(Options.Will.MessageExpiryInterval shr 24),
          Byte(Options.Will.MessageExpiryInterval shr 16),
          Byte(Options.Will.MessageExpiryInterval shr 8),
          Byte(Options.Will.MessageExpiryInterval)];
      if Options.Will.ContentType <> '' then
        Props := Props + [Byte(propContentType)] + EncodeStr(Options.Will.ContentType);
      if Options.Will.ResponseTopic <> '' then
        Props := Props + [Byte(propResponseTopic)] + EncodeStr(Options.Will.ResponseTopic);
      if Length(Options.Will.CorrelationData) > 0 then
        Props := Props + [Byte(propCorrelationData)] + EncodeBinary(Options.Will.CorrelationData);
      PL := PL + EncodeLen(Length(Props)) + Props;
    end;
    PL := PL + EncodeStr(Options.Will.Topic);
    PL := PL + EncodeBinary(Options.Will.Payload);
  end;

  // Username and Password
  if Options.Username <> '' then
    PL := PL + EncodeStr(Options.Username);
  if Options.Password <> '' then
    PL := PL + EncodeStr(Options.Password);

  RL := EncodeLen(Length(VH) + Length(PL));
  Result := [$10] + RL + VH + PL;
end;

class function TMQTTProtocol.BuildConnect(Version: Byte; const ClientID: string;
  KeepAlive: Word; Clean: Boolean; const Will: TMQTTWillOptions): TBytes;
var
  Options: TMQTTConnectOptions;
begin
  Options.SetDefaults;
  Options.ClientID := ClientID;
  Options.KeepAliveSec := KeepAlive;
  Options.CleanStart := Clean;
  if Version = 4 then
    Options.Version := MQTT311
  else
    Options.Version := MQTT5;
  Options.Will := Will;
  Result := BuildConnect(Options);
end;

class function TMQTTProtocol.BuildPublish(Version: Byte; const Topic: string;
  const Payload: TBytes; QoS: TMQTTQoS; Retain, Dup: Boolean; PacketID: Word): TBytes;
var
  VH, RL: TBytes;
  Header: Byte;
begin
  VH := EncodeStr(Topic);
  if QoS > atMostOnce then
    VH := VH + [Hi(PacketID), Lo(PacketID)];

  // Properties (MQTT 5.0) - empty for now
  if Version = 5 then
    VH := VH + [0];

  RL := EncodeLen(Length(VH) + Length(Payload));

  // Fixed Header
  Header := Ord(ptPublish) shl 4;
  if Dup then
    Header := Header or $08;
  Header := Header or (Ord(QoS) shl 1);
  if Retain then
    Header := Header or $01;

  Result := [Header] + RL + VH + Payload;
end;

class function TMQTTProtocol.BuildSubscribe(Version: Byte; PacketID: Word;
  const Topics: TMQTTSubscriptions): TBytes;
var
  VH, RL, PL: TBytes;
  I: Integer;
  SubOpts: Byte;
begin
  VH := [Hi(PacketID), Lo(PacketID)];
  if Version = 5 then
    VH := VH + [0]; // Empty properties

  SetLength(PL, 0);
  for I := 0 to High(Topics) do
  begin
    PL := PL + EncodeStr(Topics[I].Topic);
    if Version = 5 then
    begin
      SubOpts := Ord(Topics[I].QoS);
      if Topics[I].NoLocal then
        SubOpts := SubOpts or $04;
      if Topics[I].RetainAsPublished then
        SubOpts := SubOpts or $08;
      SubOpts := SubOpts or ((Topics[I].RetainHandling and $03) shl 4);
      PL := PL + [SubOpts];
    end
    else
      PL := PL + [Ord(Topics[I].QoS)];
  end;

  RL := EncodeLen(Length(VH) + Length(PL));
  Result := [$82] + RL + VH + PL;
end;

class function TMQTTProtocol.BuildSubscribe(Version: Byte; PacketID: Word;
  const Topic: string; QoS: TMQTTQoS): TBytes;
var
  Topics: TMQTTSubscriptions;
begin
  SetLength(Topics, 1);
  Topics[0].Topic := Topic;
  Topics[0].QoS := QoS;
  Topics[0].NoLocal := False;
  Topics[0].RetainAsPublished := False;
  Topics[0].RetainHandling := 0;
  Result := BuildSubscribe(Version, PacketID, Topics);
end;

class function TMQTTProtocol.BuildUnsubscribe(Version: Byte; PacketID: Word;
  const Topics: TArray<string>): TBytes;
var
  VH, RL, PL: TBytes;
  I: Integer;
begin
  VH := [Hi(PacketID), Lo(PacketID)];
  if Version = 5 then
    VH := VH + [0]; // Empty properties

  SetLength(PL, 0);
  for I := 0 to High(Topics) do
    PL := PL + EncodeStr(Topics[I]);

  RL := EncodeLen(Length(VH) + Length(PL));
  Result := [$A2] + RL + VH + PL;
end;

class function TMQTTProtocol.BuildPubAck(Version: Byte; PacketID: Word; ReasonCode: Byte): TBytes;
begin
  if (Version = 5) and (ReasonCode <> 0) then
    Result := [$40, 4, Hi(PacketID), Lo(PacketID), ReasonCode, 0]
  else if Version = 5 then
    Result := [$40, 2, Hi(PacketID), Lo(PacketID)]
  else
    Result := [$40, 2, Hi(PacketID), Lo(PacketID)];
end;

class function TMQTTProtocol.BuildPubRec(Version: Byte; PacketID: Word; ReasonCode: Byte): TBytes;
begin
  if (Version = 5) and (ReasonCode <> 0) then
    Result := [$50, 4, Hi(PacketID), Lo(PacketID), ReasonCode, 0]
  else if Version = 5 then
    Result := [$50, 2, Hi(PacketID), Lo(PacketID)]
  else
    Result := [$50, 2, Hi(PacketID), Lo(PacketID)];
end;

class function TMQTTProtocol.BuildPubRel(Version: Byte; PacketID: Word; ReasonCode: Byte): TBytes;
begin
  if (Version = 5) and (ReasonCode <> 0) then
    Result := [$62, 4, Hi(PacketID), Lo(PacketID), ReasonCode, 0]
  else if Version = 5 then
    Result := [$62, 2, Hi(PacketID), Lo(PacketID)]
  else
    Result := [$62, 2, Hi(PacketID), Lo(PacketID)];
end;

class function TMQTTProtocol.BuildPubComp(Version: Byte; PacketID: Word; ReasonCode: Byte): TBytes;
begin
  if (Version = 5) and (ReasonCode <> 0) then
    Result := [$70, 4, Hi(PacketID), Lo(PacketID), ReasonCode, 0]
  else if Version = 5 then
    Result := [$70, 2, Hi(PacketID), Lo(PacketID)]
  else
    Result := [$70, 2, Hi(PacketID), Lo(PacketID)];
end;

class function TMQTTProtocol.BuildPingReq: TBytes;
begin
  Result := [$C0, $00];
end;

class function TMQTTProtocol.BuildPingResp: TBytes;
begin
  Result := [$D0, $00];
end;

class function TMQTTProtocol.BuildDisconnect(Version: Byte; ReasonCode: Byte): TBytes;
begin
  if (Version = 5) and (ReasonCode <> 0) then
    Result := [$E0, 2, ReasonCode, 0]
  else if Version = 5 then
    Result := [$E0, 0]
  else
    Result := [$E0, 0];
end;

class function TMQTTProtocol.ParseConnAck(const Packet: TBytes; Version: Byte;
  out SessionPresent: Boolean; out ReasonCode: Byte): Boolean;
var
  Offset: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $20) then
    Exit;

  Offset := 1;
  DecodeLen(Packet, Offset); // Skip remaining length

  if Offset >= Length(Packet) then
    Exit;

  SessionPresent := (Packet[Offset] and $01) <> 0;
  Inc(Offset);

  if Offset >= Length(Packet) then
    Exit;

  ReasonCode := Packet[Offset];
  Result := True;
end;

class function TMQTTProtocol.ParsePublish(const Packet: TBytes; Version: Byte;
  out Topic: string; out Payload: TBytes; out QoS: TMQTTQoS; out Retain, Dup: Boolean;
  out PacketID: Word): Boolean;
var
  Offset, RemLen, PropsLen, PayloadStart: Integer;
  Header: Byte;
begin
  Result := False;
  if (Length(Packet) < 2) or ((Packet[0] and $F0) <> $30) then
    Exit;

  Header := Packet[0];
  Dup := (Header and $08) <> 0;
  QoS := TMQTTQoS((Header and $06) shr 1);
  Retain := (Header and $01) <> 0;

  Offset := 1;
  RemLen := DecodeLen(Packet, Offset);

  PayloadStart := Offset + RemLen;
  if PayloadStart > Length(Packet) then
    Exit;

  Topic := DecodeStr(Packet, Offset);

  PacketID := 0;
  if QoS > atMostOnce then
  begin
    if Offset + 2 > Length(Packet) then
      Exit;
    PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
    Inc(Offset, 2);
  end;

  // Properties (MQTT 5.0)
  if Version = 5 then
  begin
    PropsLen := DecodeLen(Packet, Offset);
    Inc(Offset, PropsLen); // Skip properties for now
  end;

  // Payload
  SetLength(Payload, PayloadStart - Offset);
  if Length(Payload) > 0 then
    Move(Packet[Offset], Payload[0], Length(Payload));

  Result := True;
end;

class function TMQTTProtocol.ParseSubAck(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCodes: TArray<Byte>): Boolean;
var
  Offset, PropsLen, I: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $90) then
    Exit;

  Offset := 1;
  DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  // Properties (MQTT 5.0)
  if Version = 5 then
  begin
    PropsLen := DecodeLen(Packet, Offset);
    Inc(Offset, PropsLen);
  end;

  // Reason Codes
  SetLength(ReasonCodes, Length(Packet) - Offset);
  for I := 0 to High(ReasonCodes) do
    ReasonCodes[I] := Packet[Offset + I];

  Result := True;
end;

class function TMQTTProtocol.ParseUnsubAck(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCodes: TArray<Byte>): Boolean;
var
  Offset, PropsLen, I: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $B0) then
    Exit;

  Offset := 1;
  DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  // Properties (MQTT 5.0)
  if Version = 5 then
  begin
    PropsLen := DecodeLen(Packet, Offset);
    Inc(Offset, PropsLen);
  end;

  // Reason Codes
  SetLength(ReasonCodes, Length(Packet) - Offset);
  for I := 0 to High(ReasonCodes) do
    ReasonCodes[I] := Packet[Offset + I];

  Result := True;
end;

class function TMQTTProtocol.ParsePubAck(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCode: Byte): Boolean;
var
  Offset, RemLen: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $40) then
    Exit;

  Offset := 1;
  RemLen := DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  if (Version = 5) and (RemLen >= 3) and (Offset < Length(Packet)) then
    ReasonCode := Packet[Offset]
  else
    ReasonCode := 0;

  Result := True;
end;

class function TMQTTProtocol.ParsePubRec(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCode: Byte): Boolean;
var
  Offset, RemLen: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $50) then
    Exit;

  Offset := 1;
  RemLen := DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  if (Version = 5) and (RemLen >= 3) and (Offset < Length(Packet)) then
    ReasonCode := Packet[Offset]
  else
    ReasonCode := 0;

  Result := True;
end;

class function TMQTTProtocol.ParsePubRel(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCode: Byte): Boolean;
var
  Offset, RemLen: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $60) then
    Exit;

  Offset := 1;
  RemLen := DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  if (Version = 5) and (RemLen >= 3) and (Offset < Length(Packet)) then
    ReasonCode := Packet[Offset]
  else
    ReasonCode := 0;

  Result := True;
end;

class function TMQTTProtocol.ParsePubComp(const Packet: TBytes; Version: Byte;
  out PacketID: Word; out ReasonCode: Byte): Boolean;
var
  Offset, RemLen: Integer;
begin
  Result := False;
  if (Length(Packet) < 4) or ((Packet[0] and $F0) <> $70) then
    Exit;

  Offset := 1;
  RemLen := DecodeLen(Packet, Offset);

  if Offset + 2 > Length(Packet) then
    Exit;

  PacketID := (Packet[Offset] shl 8) or Packet[Offset + 1];
  Inc(Offset, 2);

  if (Version = 5) and (RemLen >= 3) and (Offset < Length(Packet)) then
    ReasonCode := Packet[Offset]
  else
    ReasonCode := 0;

  Result := True;
end;

class function TMQTTProtocol.TopicMatchesFilter(const Topic, Filter: string): Boolean;
var
  TopicParts, FilterParts: TArray<string>;
  I: Integer;
begin
  // Exact match
  if Topic = Filter then
    Exit(True);

  // Split into parts
  TopicParts := Topic.Split(['/']);
  FilterParts := Filter.Split(['/']);

  I := 0;
  while (I < Length(FilterParts)) do
  begin
    // Multi-level wildcard - matches everything from here
    if FilterParts[I] = '#' then
      Exit(True);

    // No more topic parts but filter continues (and not #)
    if I >= Length(TopicParts) then
      Exit(False);

    // Single-level wildcard - matches this level
    if FilterParts[I] = '+' then
    begin
      Inc(I);
      Continue;
    end;

    // Literal match required
    if FilterParts[I] <> TopicParts[I] then
      Exit(False);

    Inc(I);
  end;

  // Filter exhausted, check if topic also exhausted
  Result := I = Length(TopicParts);
end;

class function TMQTTProtocol.IsValidTopicFilter(const Filter: string): Boolean;
var
  Parts: TArray<string>;
  I: Integer;
begin
  if Filter = '' then
    Exit(False);

  Parts := Filter.Split(['/']);

  for I := 0 to High(Parts) do
  begin
    // # must be the last part
    if Parts[I] = '#' then
      Exit(I = High(Parts));

    // + is valid anywhere as single level
    if Parts[I] = '+' then
      Continue;

    // No wildcards allowed in the middle of a part
    if (Pos('+', Parts[I]) > 0) or (Pos('#', Parts[I]) > 0) then
      Exit(False);
  end;

  Result := True;
end;

class function TMQTTProtocol.IsValidTopicName(const Topic: string): Boolean;
begin
  // Topic names cannot contain wildcards
  if Topic = '' then
    Exit(False);
  if Pos('+', Topic) > 0 then
    Exit(False);
  if Pos('#', Topic) > 0 then
    Exit(False);
  Result := True;
end;

class function TMQTTProtocol.GetPacketType(const Packet: TBytes): TMQTTPacketType;
begin
  if Length(Packet) = 0 then
    Result := ptReserved
  else
    Result := TMQTTPacketType(Packet[0] shr 4);
end;

class function TMQTTProtocol.GetPacketID(const Packet: TBytes): Word;
var
  Offset: Integer;
  PacketType: TMQTTPacketType;
begin
  Result := 0;
  if Length(Packet) < 4 then
    Exit;

  PacketType := GetPacketType(Packet);

  Offset := 1;
  DecodeLen(Packet, Offset);

  case PacketType of
    ptPubAck,
    ptPubRec,
    ptPubRel,
    ptPubComp,
    ptSubscribe,
    ptSubAck,
    ptUnsubscribe,
    ptUnsubAck:
      if Offset + 2 <= Length(Packet) then
        Result := (Packet[Offset] shl 8) or Packet[Offset + 1];

    ptPublish:
      begin
        // Skip topic
        if Offset + 2 > Length(Packet) then
          Exit;
        Offset := Offset + 2 + (Packet[Offset] shl 8) or Packet[Offset + 1];
        // QoS > 0 has packet ID
        if ((Packet[0] and $06) <> 0) and (Offset + 2 <= Length(Packet)) then
          Result := (Packet[Offset] shl 8) or Packet[Offset + 1];
      end;
  end;
end;

end.
