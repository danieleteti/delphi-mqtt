#!/usr/bin/env python3
"""
Generator for minimal Delphi console-app .dproj files with Win32+Win64
enabled and Win64 build targets wired in.

Used by scripts/regen_dprojs.py to (re)create the dproj files that don't
exist in version control yet (tests, 11_Logging, 13_LoggerPro, cross-protocol
helpers, etc.).
"""

from __future__ import annotations
import uuid


def make_dproj(*, main_source: str, project_name: str, dcc_references: list[str],
               unit_search_path: str = "", extra_namespaces: str = "") -> str:
    """Return the XML text of a minimal Win32+Win64 console .dproj.

    main_source:       relative path to the .dpr (e.g. "LoggerProDemo.dpr")
    project_name:      project name (e.g. "LoggerProDemo")
    dcc_references:    list of relative paths to .pas units explicitly listed
                       in the ItemGroup. Compile order matches the list.
    unit_search_path:  semicolon-separated extra directories for DCC_UnitSearchPath
    extra_namespaces:  extra namespaces to prepend to DCC_Namespace
    """
    guid = str(uuid.uuid4()).upper()
    refs_xml = "\n".join(
        f'        <DCCReference Include="{p}"/>' for p in dcc_references
    )
    search_xml = ""
    if unit_search_path:
        search_xml = (
            f"\n        <DCC_UnitSearchPath>{unit_search_path};"
            "$(DCC_UnitSearchPath)</DCC_UnitSearchPath>"
        )
    ns_prefix = ""
    if extra_namespaces:
        ns_prefix = f"{extra_namespaces};"

    return f"""<Project xmlns="http://schemas.microsoft.com/developer/msbuild/2003">
    <PropertyGroup>
        <ProjectGuid>{{{guid}}}</ProjectGuid>
        <MainSource>{main_source}</MainSource>
        <Base>True</Base>
        <Config Condition="'$(Config)'==''">Debug</Config>
        <ProjectName Condition="'$(ProjectName)'==''">{project_name}</ProjectName>
        <AppType>Console</AppType>
        <FrameworkType>None</FrameworkType>
        <ProjectVersion>20.4</ProjectVersion>
        <Platform Condition="'$(Platform)'==''">Win64</Platform>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Config)'=='Base' or '$(Base)'!=''">
        <Base>true</Base>
    </PropertyGroup>
    <PropertyGroup Condition="('$(Platform)'=='Win32' and '$(Base)'=='true') or '$(Base_Win32)'!=''">
        <Base_Win32>true</Base_Win32>
        <CfgParent>Base</CfgParent>
        <Base>true</Base>
    </PropertyGroup>
    <PropertyGroup Condition="('$(Platform)'=='Win64' and '$(Base)'=='true') or '$(Base_Win64)'!=''">
        <Base_Win64>true</Base_Win64>
        <CfgParent>Base</CfgParent>
        <Base>true</Base>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Config)'=='Release' or '$(Cfg_1)'!=''">
        <Cfg_1>true</Cfg_1>
        <CfgParent>Base</CfgParent>
        <Base>true</Base>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Config)'=='Debug' or '$(Cfg_2)'!=''">
        <Cfg_2>true</Cfg_2>
        <CfgParent>Base</CfgParent>
        <Base>true</Base>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Base)'!=''">
        <DCC_E>false</DCC_E>
        <DCC_F>false</DCC_F>
        <DCC_K>false</DCC_K>
        <DCC_N>false</DCC_N>
        <DCC_S>false</DCC_S>
        <DCC_ImageBase>00400000</DCC_ImageBase>
        <SanitizedProjectName>{project_name}</SanitizedProjectName>
        <VerInfo_Locale>1033</VerInfo_Locale>
        <DCC_Namespace>{ns_prefix}System;Xml;Data;Datasnap;Web;Soap;$(DCC_Namespace)</DCC_Namespace>{search_xml}
    </PropertyGroup>
    <PropertyGroup Condition="'$(Base_Win32)'!=''">
        <DCC_Namespace>Winapi;System.Win;Data.Win;Datasnap.Win;Web.Win;Soap.Win;Xml.Win;$(DCC_Namespace)</DCC_Namespace>
        <BT_BuildType>Debug</BT_BuildType>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Base_Win64)'!=''">
        <DCC_Namespace>Winapi;System.Win;Data.Win;Datasnap.Win;Web.Win;Soap.Win;Xml.Win;$(DCC_Namespace)</DCC_Namespace>
        <BT_BuildType>Debug</BT_BuildType>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Cfg_1)'!=''">
        <DCC_Define>RELEASE;$(DCC_Define)</DCC_Define>
        <DCC_DebugInformation>0</DCC_DebugInformation>
        <DCC_LocalDebugSymbols>false</DCC_LocalDebugSymbols>
    </PropertyGroup>
    <PropertyGroup Condition="'$(Cfg_2)'!=''">
        <DCC_Define>DEBUG;$(DCC_Define)</DCC_Define>
        <DCC_Optimize>false</DCC_Optimize>
        <DCC_GenerateStackFrames>true</DCC_GenerateStackFrames>
    </PropertyGroup>
    <ItemGroup>
        <DelphiCompile Include="$(MainSource)">
            <MainSource>MainSource</MainSource>
        </DelphiCompile>
{refs_xml}
        <BuildConfiguration Include="Base">
            <Key>Base</Key>
        </BuildConfiguration>
        <BuildConfiguration Include="Release">
            <Key>Cfg_1</Key>
            <CfgParent>Base</CfgParent>
        </BuildConfiguration>
        <BuildConfiguration Include="Debug">
            <Key>Cfg_2</Key>
            <CfgParent>Base</CfgParent>
        </BuildConfiguration>
    </ItemGroup>
    <ProjectExtensions>
        <Borland.Personality>Delphi.Personality.12</Borland.Personality>
        <Borland.ProjectType/>
        <BorlandProject>
            <Delphi.Personality>
                <Source>
                    <Source Name="MainSource">{main_source}</Source>
                </Source>
            </Delphi.Personality>
            <Platforms>
                <Platform value="Win32">True</Platform>
                <Platform value="Win64">True</Platform>
            </Platforms>
        </BorlandProject>
        <ProjectFileVersion>12</ProjectFileVersion>
    </ProjectExtensions>
    <Import Project="$(BDS)\\Bin\\CodeGear.Delphi.Targets" Condition="Exists('$(BDS)\\Bin\\CodeGear.Delphi.Targets')"/>
</Project>
"""
