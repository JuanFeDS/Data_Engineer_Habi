"""
Tests for the main module.
"""

from unittest.mock import patch
from app.main import main


def test_main_successful_execution():
    """Test that main function executes successfully."""
    with patch("builtins.print") as mock_print:
        main()
        mock_print.assert_called_once_with("Hello World")


def test_main_handles_exception():
    """Test that main function handles exceptions properly."""
    with patch("builtins.print") as mock_print:
        # First call raises exception, second call handles the error
        mock_print.side_effect = [Exception("Test error"), None]
        main()
        # Should be called twice: once for "Hello World" (raises exception), once for error message
        assert mock_print.call_count == 2
        mock_print.assert_any_call("Error: Test error")
