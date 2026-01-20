object frmMain: TfrmMain
  Left = 0
  Top = 0
  Caption = 'MQTT Chat'
  ClientHeight = 450
  ClientWidth = 650
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  Position = poScreenCenter
  OnCloseQuery = FormCloseQuery
  OnCreate = FormCreate
  OnDestroy = FormDestroy
  TextHeight = 15
  object Splitter1: TSplitter
    Left = 510
    Top = 65
    Width = 5
    Height = 324
    Align = alRight
    ExplicitLeft = 505
    ExplicitTop = 70
    ExplicitHeight = 319
  end
  object pnlTop: TPanel
    Left = 0
    Top = 0
    Width = 650
    Height = 65
    Align = alTop
    BevelOuter = bvNone
    TabOrder = 0
    ExplicitTop = -1
    DesignSize = (
      650
      65)
    object lblHost: TLabel
      Left = 12
      Top = 12
      Width = 25
      Height = 15
      Caption = 'Host'
    end
    object lblPort: TLabel
      Left = 175
      Top = 12
      Width = 22
      Height = 15
      Caption = 'Port'
    end
    object lblNickname: TLabel
      Left = 246
      Top = 12
      Width = 54
      Height = 15
      Caption = 'Nickname'
    end
    object lblRoom: TLabel
      Left = 442
      Top = 12
      Width = 32
      Height = 15
      Anchors = [akTop, akRight]
      Caption = 'Room'
    end
    object edtHost: TEdit
      Left = 12
      Top = 31
      Width = 150
      Height = 23
      TabOrder = 0
      Text = 'localhost'
    end
    object edtPort: TEdit
      Left = 175
      Top = 31
      Width = 55
      Height = 23
      TabOrder = 1
      Text = '1883'
    end
    object edtNickname: TEdit
      Left = 246
      Top = 31
      Width = 100
      Height = 23
      TabOrder = 2
    end
    object btnConnect: TButton
      Left = 356
      Top = 29
      Width = 60
      Height = 25
      Caption = 'Connect'
      TabOrder = 3
      OnClick = btnConnectClick
    end
    object btnDisconnect: TButton
      Left = 352
      Top = 29
      Width = 68
      Height = 25
      Caption = 'Disconnect'
      TabOrder = 4
      Visible = False
      OnClick = btnDisconnectClick
    end
    object edtRoom: TEdit
      Left = 442
      Top = 31
      Width = 90
      Height = 23
      Anchors = [akTop, akRight]
      TabOrder = 5
      Text = 'general'
    end
    object btnJoin: TButton
      Left = 538
      Top = 29
      Width = 50
      Height = 25
      Anchors = [akTop, akRight]
      Caption = 'Join'
      TabOrder = 6
      OnClick = btnJoinClick
    end
    object btnLeave: TButton
      Left = 594
      Top = 29
      Width = 50
      Height = 25
      Anchors = [akTop, akRight]
      Caption = 'Leave'
      TabOrder = 7
      OnClick = btnLeaveClick
    end
  end
  object pnlBottom: TPanel
    Left = 0
    Top = 389
    Width = 650
    Height = 42
    Align = alBottom
    BevelOuter = bvNone
    TabOrder = 1
    DesignSize = (
      650
      42)
    object lblMessage: TLabel
      Left = 12
      Top = 13
      Width = 46
      Height = 15
      Caption = 'Message'
    end
    object edtMessage: TEdit
      Left = 75
      Top = 10
      Width = 488
      Height = 23
      Anchors = [akLeft, akTop, akRight]
      TabOrder = 0
      OnKeyPress = edtMessageKeyPress
    end
    object btnSend: TButton
      Left = 569
      Top = 8
      Width = 70
      Height = 25
      Anchors = [akTop, akRight]
      Caption = 'Send'
      Default = True
      TabOrder = 1
      OnClick = btnSendClick
    end
  end
  object pnlMessages: TPanel
    Left = 0
    Top = 65
    Width = 510
    Height = 324
    Align = alClient
    BevelOuter = bvNone
    TabOrder = 2
    object memoMessages: TMemo
      Left = 0
      Top = 0
      Width = 510
      Height = 324
      Align = alClient
      ReadOnly = True
      ScrollBars = ssVertical
      TabOrder = 0
    end
  end
  object StatusBar: TStatusBar
    Left = 0
    Top = 431
    Width = 650
    Height = 19
    Panels = <>
    SimplePanel = True
    SimpleText = 'Disconnected'
  end
  object pnlRight: TPanel
    Left = 515
    Top = 65
    Width = 135
    Height = 324
    Align = alRight
    BevelOuter = bvNone
    TabOrder = 4
    object lblOnline: TLabel
      Left = 8
      Top = 5
      Width = 35
      Height = 15
      Caption = 'Online'
    end
    object lstOnline: TListBox
      Left = 0
      Top = 25
      Width = 135
      Height = 299
      Align = alBottom
      Anchors = [akLeft, akTop, akRight, akBottom]
      ItemHeight = 15
      TabOrder = 0
    end
  end
end
