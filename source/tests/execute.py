"""
Integration Tests
"""

# test operators

# test sanitizer

# test json serialization


"""
This tests that a very simple flow is able to be created and execute.
"""
import simpleflow

assert simpleflow.execute(), "Failed to execute a simple flow"
