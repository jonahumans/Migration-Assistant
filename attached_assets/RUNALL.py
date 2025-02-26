import subprocess

def run_all_scripts():
    # List of scripts to run
    scripts = [
        "addvariants.py",
        "parentattributesonvarients.py",
        "variantattributes.py"
    ]
    
    # Run each script
    for script in scripts:
        print(f"Running {script}...")
        subprocess.run(["python3", script], check=True)
        print(f"{script} finished.")

if __name__ == "__main__":
    run_all_scripts()
