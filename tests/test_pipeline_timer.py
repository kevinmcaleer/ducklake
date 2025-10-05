"""Tests for pipeline timing utilities."""
import pytest
import time
from ducklake_core.pipeline_timer import PipelineTimer


class TestPipelineTimer:
    """Test pipeline timing functionality."""

    def test_pipeline_timer_creation(self):
        """Test creating a pipeline timer."""
        timer = PipelineTimer()
        assert isinstance(timer.phases, dict)
        assert len(timer.phases) == 0
        assert timer.start_time > 0

    def test_phase_context_manager(self):
        """Test using phase as context manager."""
        timer = PipelineTimer()

        with timer.phase('test_phase'):
            time.sleep(0.01)  # Small delay to measure

        # Should have recorded the phase
        assert 'test_phase_s' in timer.phases
        assert timer.phases['test_phase_s'] > 0
        assert timer.phases['test_phase_s'] < 1  # Should be small

    def test_multiple_phases(self):
        """Test timing multiple phases."""
        timer = PipelineTimer()

        with timer.phase('phase1'):
            time.sleep(0.01)

        with timer.phase('phase2'):
            time.sleep(0.01)

        # Should have both phases
        assert 'phase1_s' in timer.phases
        assert 'phase2_s' in timer.phases
        assert len(timer.phases) == 2

    def test_phase_with_exception(self):
        """Test that timing works even when phase raises exception."""
        timer = PipelineTimer()

        with pytest.raises(ValueError):
            with timer.phase('error_phase'):
                time.sleep(0.01)
                raise ValueError("Test error")

        # Should still record timing
        assert 'error_phase_s' in timer.phases
        assert timer.phases['error_phase_s'] > 0

    def test_get_total_time(self):
        """Test getting total elapsed time."""
        timer = PipelineTimer()
        time.sleep(0.01)

        total = timer.get_total_time()
        assert total > 0
        assert total < 1  # Should be small

    def test_get_summary(self):
        """Test getting timing summary."""
        timer = PipelineTimer()

        with timer.phase('test'):
            time.sleep(0.01)

        summary = timer.get_summary()

        # Should include phases and total
        assert 'test_s' in summary
        assert 'total_s' in summary
        assert summary['total_s'] >= summary['test_s']

    def test_phase_timing_precision(self):
        """Test that phase timing has reasonable precision."""
        timer = PipelineTimer()

        with timer.phase('quick'):
            pass  # Very quick operation

        # Should record even very quick operations
        assert 'quick_s' in timer.phases
        # Should be 0 or very small number
        assert timer.phases['quick_s'] >= 0

    def test_nested_phases_not_supported(self):
        """Test that nested phases work independently."""
        timer = PipelineTimer()

        with timer.phase('outer'):
            time.sleep(0.01)
            with timer.phase('inner'):
                time.sleep(0.01)

        # Both phases should be recorded
        assert 'outer_s' in timer.phases
        assert 'inner_s' in timer.phases
        # Outer should be longer than inner
        assert timer.phases['outer_s'] >= timer.phases['inner_s']

    def test_phase_name_formatting(self):
        """Test that phase names are formatted consistently."""
        timer = PipelineTimer()

        with timer.phase('my_phase'):
            pass

        with timer.phase('another-phase'):
            pass

        # Should append '_s' suffix
        assert 'my_phase_s' in timer.phases
        assert 'another-phase_s' in timer.phases

    def test_timer_reuse(self):
        """Test that timer can be reused for multiple measurements."""
        timer = PipelineTimer()

        # First measurement
        with timer.phase('first'):
            time.sleep(0.01)

        first_total = timer.get_total_time()

        # Second measurement
        time.sleep(0.01)
        with timer.phase('second'):
            time.sleep(0.01)

        second_total = timer.get_total_time()

        # Should accumulate
        assert second_total > first_total
        assert 'first_s' in timer.phases
        assert 'second_s' in timer.phases

    def test_summary_immutability(self):
        """Test that summary is a snapshot, not a reference."""
        timer = PipelineTimer()

        with timer.phase('test'):
            time.sleep(0.01)

        summary1 = timer.get_summary()

        # Add another phase
        with timer.phase('test2'):
            time.sleep(0.01)

        summary2 = timer.get_summary()

        # First summary should not have changed
        assert 'test2_s' not in summary1
        assert 'test2_s' in summary2
        assert summary2['total_s'] > summary1['total_s']


class TestPipelineTimerIntegration:
    """Integration tests for pipeline timer."""

    def test_realistic_pipeline_timing(self):
        """Test timing a realistic pipeline scenario."""
        timer = PipelineTimer()

        # Simulate pipeline phases
        with timer.phase('ingest'):
            time.sleep(0.02)

        with timer.phase('transform'):
            time.sleep(0.03)

        with timer.phase('load'):
            time.sleep(0.01)

        summary = timer.get_summary()

        # Validate structure
        expected_phases = ['ingest_s', 'transform_s', 'load_s', 'total_s']
        for phase in expected_phases:
            assert phase in summary
            assert summary[phase] > 0

        # Validate relationships
        assert summary['total_s'] >= summary['transform_s']  # Total >= longest phase
        assert summary['transform_s'] > summary['ingest_s']  # Transform took longer
        assert summary['ingest_s'] > summary['load_s']       # Ingest took longer than load

    def test_error_recovery_timing(self):
        """Test timing when some phases fail."""
        timer = PipelineTimer()

        # Successful phase
        with timer.phase('setup'):
            time.sleep(0.01)

        # Failed phase
        try:
            with timer.phase('process'):
                time.sleep(0.01)
                raise RuntimeError("Processing failed")
        except RuntimeError:
            pass

        # Recovery phase
        with timer.phase('cleanup'):
            time.sleep(0.01)

        summary = timer.get_summary()

        # All phases should be timed, even the failed one
        assert 'setup_s' in summary
        assert 'process_s' in summary
        assert 'cleanup_s' in summary
        assert 'total_s' in summary

        # Total should be sum of all phases (approximately)
        phase_sum = summary['setup_s'] + summary['process_s'] + summary['cleanup_s']
        assert abs(summary['total_s'] - phase_sum) < 0.01  # Allow small timing differences