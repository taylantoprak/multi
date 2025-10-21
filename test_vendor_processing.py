#!/usr/bin/env python3
"""
Test script to verify that all vendors are processed correctly.
This script will run the 9images.py with a small subset of vendors to test the fix.
"""

import subprocess
import sys
import os
import time

def test_vendor_processing():
    print("🧪 Testing Vendor Processing Fix")
    print("=" * 50)
    
    # Sample dates for testing
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    
    print(f"📅 Date range: {start_date} to {end_date}")
    print("🔧 Testing with all 63 vendors...")
    print("=" * 50)
    
    # Prepare input for the script
    input_data = f"{start_date}\n{end_date}\n"
    
    try:
        print("🚀 Starting 9images.py...")
        start_time = time.time()
        
        # Run the optimized 9images script
        process = subprocess.Popen(
            [sys.executable, "9images.py"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        stdout, stderr = process.communicate(input=input_data)
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"⏱️  Total execution time: {duration:.1f} seconds")
        print(f"📊 Exit code: {process.returncode}")
        
        # Check for key indicators in output
        if "All vendor processing completed" in stdout:
            print("✅ SUCCESS: All vendors were processed!")
        else:
            print("❌ WARNING: May not have processed all vendors")
        
        if "Started downloads for" in stdout:
            # Extract the number of vendors that started downloads
            import re
            match = re.search(r"Started downloads for (\d+) vendors", stdout)
            if match:
                download_count = int(match.group(1))
                print(f"📥 Downloads started for {download_count} vendors")
                
                if download_count >= 30:  # Should be around 44 based on previous runs
                    print("✅ GOOD: Reasonable number of vendors with downloads")
                else:
                    print("⚠️  LOW: Fewer vendors with downloads than expected")
        
        # Check for errors
        if stderr:
            print("\n⚠️  ERRORS/WARNINGS:")
            print(stderr[:500] + "..." if len(stderr) > 500 else stderr)
        
        print("\n📋 SUMMARY:")
        print(f"   • Execution time: {duration:.1f}s")
        print(f"   • Exit code: {process.returncode}")
        print(f"   • All vendors processed: {'✅' if 'All vendor processing completed' in stdout else '❌'}")
        
    except Exception as e:
        print(f"❌ Error running test: {e}")

if __name__ == "__main__":
    test_vendor_processing()