"""
Integration Tests
"""

# test operators

# test sanitizer

"""
This tests that JSON serialization routines are working as expected
"""
import serialize

print(">>> EXECUTING SERIALIZATION TESTS")
serialize.execute()

"""
This tests that a very simple flow is able to be created and execute.
"""
import simpleflow

print(">>> EXECUTING SIMPLE FLOW")
simpleflow.execute()
