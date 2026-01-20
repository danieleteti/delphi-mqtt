import subprocess
import sys
import os

RSVARS_PATH = r"C:\Program Files (x86)\Embarcadero\Studio\37.0\bin\rsvars.bat"

def build_project(project_path, search_paths=None):
    if not os.path.exists(project_path):
        print(f"Error: Project file not found: {project_path}")
        return False

    cmd_parts = [f'call "{RSVARS_PATH}"']
    
    dcc32_cmd = f"dcc32 -Q -B"
    if search_paths:
        dcc32_cmd += f' -U"{search_paths}"'
    
    dcc32_cmd += f' "{project_path}"'
    
    cmd_parts.append(dcc32_cmd)
    
    full_command = " && ".join(cmd_parts)
    
    print(f"Building: {project_path}...")
    # print(f"Command: {full_command}")
    
    try:
        # Use shell=True to allow the && operator and batch file execution
        result = subprocess.run(full_command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print(result.stdout)
        print("Build Successful.")
        return True
    except subprocess.CalledProcessError as e:
        print("Build Failed.")
        print(e.stdout)
        print(e.stderr)
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python build.py <project_path> [search_paths]")
        sys.exit(1)
        
    project = sys.argv[1]
    paths = sys.argv[2] if len(sys.argv) > 2 else "src"
    
    if not build_project(project, paths):
        sys.exit(1)
