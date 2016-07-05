from __future__ import print_function

import sys
import collections

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject

GObject.threads_init()
Gst.init(None)

DEFAULT_STREAMER_PIPELINE = " ! ".join([
    "videotestsrc",
    "jpegenc",
    "rtpjpegpay",
    ])
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '2539'

_STR = basestring if sys.version_info[0] == 2 else str


STREAM_STATE_PLAYING = Gst.State.PLAYING
STREAM_STATE_STOPPED = Gst.State.NULL


class GstStreamer(object):
    """
    A class handling the generation and streaming of multimedia data over
    the network.
    """

    def __init__(self, pipeline_string=DEFAULT_STREAMER_PIPELINE,
                 host=DEFAULT_HOST, port=DEFAULT_PORT,
                 sink_override=None):
        """
        GStreamer multimedia streamer class.
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :param host: [string] The host/IP/Multicast group to send the packets to
        :param port: [string] The port to send the packets to
        :param sink_override: [string] Overrides the default UDPSINK
        """

        self.is_playing = False

        self._pipeline_string = None
        self._sink_string = None
        self._launch_string = None

        self._pipeline = None

        self.update_sink(host, port, sink_override)
        self.parse_launch_string(pipeline_string)

        self._msg_bus = self._pipeline.get_bus()
        self._msg_bus.add_signal_watch()
        self._msg_bus.connect("message::eos", self._on_eos)
        self._msg_bus.connect("message::error", self._on_error)

    def start(self):
        """Start the multimedia streaming"""
        self._notify('info', 'Pipeline started.')
        self.is_playing = True
        self._pipeline.set_state(STREAM_STATE_PLAYING)

    def stop(self):
        """Stop multimedia streaming"""
        self._notify('info', 'Pipeline stopped.')
        self.is_playing = False
        self._pipeline.set_state(STREAM_STATE_STOPPED)

    def parse_launch_string(self, pipeline_string=DEFAULT_STREAMER_PIPELINE):
        """
        Creates a launch string using the inputted pipline string and
        the sink string (default is UDPSINK).
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        if self._sink_string is not None:
            self.build_pipeline(pipeline_string, self._sink_string)
        else:
            self._pipeline_string = pipeline_string

    def update_sink(self, host=DEFAULT_HOST, port=DEFAULT_PORT,
                    sink_override=None):
        """
        Parse the UDPSINK, or return sink_override if inputted.
        May receive lists of host_ips and ports, splits using TEE and QUEUE.
        :param host: [string] The host/IP/Multicast group to send the packets to
        :param port: [string] The port to send the packets to
        :param sink_override: [string] Overrides the default UDPSINK
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        # Generate sink strings list
        if sink_override is not None:
            sinks_list = [sink_override, ]
        elif isinstance(host, _STR) and isinstance(port, _STR):
            sinks_list = [_format_udpsink(host, port), ]
        else:
            sinks_list = [_format_udpsink(h, p) for h, p in zip(host, port)]

        # Build sink string (tee-ing if multiple sinks)
        if len(sinks_list) == 1:
            sink_string = sinks_list[0]
        else:
            tee_head = 'tee name=tp'
            tee_list = []
            for s in sinks_list:
                tee_list.append('tp. ! {0}'.format(s))
            sink_string = ' '.join([tee_head] + tee_list)

        if self._pipeline_string is not None:
            self.build_pipeline(self._pipeline_string, sink_string)
        else:
            self._sink_string = sink_string

    def build_pipeline(self, pipeline_string, sink_string):
        """Build the pipeline"""

        if self.is_playing:
            raise RuntimeError("Can't rebuild pipeline while playing")

        if self._pipeline is not None:
            self._notify('debug', "Setting pipeline state to NULL")
            self._pipeline.set_state(Gst.State.NULL)

        launch_string = '{0} ! {1}'.format(pipeline_string, sink_string)
        self._pipeline = Gst.parse_launch(launch_string)
        self._pipeline_string = pipeline_string
        self._sink_string = sink_string
        self._launch_string = launch_string

        self._notify('info', "{0} '{1}'".format(
            'Constructed new pipeline using the following launch string',
            launch_string,
        ))

    def _on_eos(self, bus, msg):
        """Handles pipeline EOS message"""
        self._notify('info', 'Reached EOS')
        self.stop()

    def _on_error(self, bus, msg):
        """Handles pipeline ERROR message"""
        err, debug = msg.parse_error()
        self._notify(
            'err', "Error in pipeline!\nError:\n{0}\n\nDebug Info:\n{1}".format(
                err, debug))
        self.stop()

    def _notify(self, severity, msg):
        """Notify event message."""
        print('{0}: {1}'.format(severity.upper(), msg))


class GstDispatcher(object):
    """
    A ROS node class handling the generation and streaming of multimedia
    data.
    """

    def __init__(self, pipeline_string=DEFAULT_STREAMER_PIPELINE,
                 host=DEFAULT_HOST, port=DEFAULT_PORT,
                 sink_override=None):
        """
        GStreamer multimedia streamer class.
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :param host: [string] The host/IP/Multicast group to send the packets to
        :param port: [string] The port to send the packets to
        :param sink_override: [string] Overrides the default UDPSINK
        """

        self.is_playing = False

        self._pipeline_string = None
        self._sink_string = None
        self._launch_string = None

        self._pipeline = None

        self.update_sink(host, port, sink_override)
        self.parse_launch_string(pipeline_string)

        self._msg_bus = self._pipeline.get_bus()
        self._msg_bus.add_signal_watch()
        self._msg_bus.connect("message::eos", self._on_eos)
        self._msg_bus.connect("message::error", self._on_error)

    def start(self):
        """Start the multimedia streaming"""
        self._notify('info', 'Pipeline started.')
        self.is_playing = True
        self._pipeline.set_state(STREAM_STATE_PLAYING)

    def stop(self):
        """Stop multimedia streaming"""
        self._notify('info', 'Pipeline stopped.')
        self.is_playing = False
        self._pipeline.set_state(STREAM_STATE_STOPPED)

    def parse_launch_string(self, pipeline_string=DEFAULT_STREAMER_PIPELINE):
        """
        Creates a launch string using the inputted pipline string and
        the sink string (default is UDPSINK).
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        if self._sink_string is not None:
            self.build_pipeline(pipeline_string, self._sink_string)
        else:
            self._pipeline_string = pipeline_string

    def update_sink(self, host=DEFAULT_HOST, port=DEFAULT_PORT,
                    sink_override=None):
        """
        Parse the UDPSINK, or return sink_override if inputted.
        May receive lists of host_ips and ports, splits using TEE and QUEUE.
        :param host: [string] The host/IP/Multicast group to send the packets to
        :param port: [string] The port to send the packets to
        :param sink_override: [string] Overrides the default UDPSINK
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        # Generate sink strings list
        if sink_override is not None:
            sinks_list = [sink_override, ]
        elif isinstance(host, _STR) and isinstance(port, _STR):
            sinks_list = [_format_udpsink(host, port), ]
        else:
            sinks_list = [_format_udpsink(h, p) for h, p in zip(host, port)]

        # Build sink string (tee-ing if multiple sinks)
        if len(sinks_list) == 1:
            sink_string = sinks_list[0]
        else:
            tee_head = 'tee name=tp'
            tee_list = []
            for s in sinks_list:
                tee_list.append('tp. ! {0}'.format(s))
            sink_string = ' '.join([tee_head] + tee_list)

        if self._pipeline_string is not None:
            self.build_pipeline(self._pipeline_string, sink_string)
        else:
            self._sink_string = sink_string

    def build_pipeline(self, pipeline_string, sink_string):
        """Build the pipeline"""

        if self.is_playing:
            raise RuntimeError("Can't rebuild pipeline while playing")

        if self._pipeline is not None:
            self._notify('debug', "Setting pipeline state to NULL")
            self._pipeline.set_state(Gst.State.NULL)

        launch_string = '{0} ! {1}'.format(pipeline_string, sink_string)
        self._pipeline = Gst.parse_launch(launch_string)
        self._pipeline_string = pipeline_string
        self._sink_string = sink_string
        self._launch_string = launch_string

        self._notify('info', "{0} '{1}'".format(
            'Constructed new pipeline using the following launch string',
            launch_string,
        ))

    def _on_eos(self, bus, msg):
        """Handles pipeline EOS message"""
        self._notify('info', 'Reached EOS')
        self.stop()

    def _on_error(self, bus, msg):
        """Handles pipeline ERROR message"""
        err, debug = msg.parse_error()
        self._notify(
            'err', "Error in pipeline!\nError:\n{0}\n\nDebug Info:\n{1}".format(
                err, debug))
        self.stop()

    def _notify(self, severity, msg):
        """Notify event message."""
        print('{0}: {1}'.format(severity.upper(), msg))


def _format_udpsink(host, port):
    """Utility func for parsing udpsink element string"""
    host_str = 'host={0}'.format(host)
    port_str = 'port={0}'.format(port)
    sink_str = 'queue ! udpsink {0} {1}'.format(host_str, port_str)
    return sink_str
