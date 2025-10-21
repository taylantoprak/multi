#!/usr/bin/env python3
"""
Demo script for optimized multivendor.py
This script runs multivendor_optimized.py with sample dates for demonstration.
"""

import subprocess
import sys
import os

def main():
    print("🚀 Running Optimized Multivendor Demo")
    print("=" * 50)
    
    # Sample dates for demonstration
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print("⚙️  Configuration: 10 concurrent vendors, async downloads, compression enabled")
    print("=" * 50)
    
    # Prepare input for the script
    input_data = f"{start_date}\n{end_date}\n"
    
    try:
        # Run the optimized multivendor script
        process = subprocess.Popen(
            [sys.executable, "multivendor_optimized.py"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate(input=input_data)
        
        print("📤 SCRIPT OUTPUT:")
        print(stdout)
        
        if stderr:
            print("⚠️  ERRORS/WARNINGS:")
            print(stderr)
        
        print(f"📊 Exit code: {process.returncode}")
        
    except Exception as e:
        print(f"❌ Error running demo: {e}")

if __name__ == "__main__":
    main()