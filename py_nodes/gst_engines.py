from __future__ import print_function

import sys
import threading
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
DEFAULT_DISPATCHER_PIPELINE = " ! ".join([
    "application/x-rtp, encoding-name=JPEG,payload=26",
    "rtpjpegdepay",
    "jpegdec",
    ])
DEFAULT_VIEWER_PIPELINE = DEFAULT_DISPATCHER_PIPELINE
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '2539'

_STR = basestring if sys.version_info[0] == 2 else str


STREAM_STATE_PLAYING = Gst.State.PLAYING
STREAM_STATE_STOPPED = Gst.State.NULL


class GstBase(object):
    """
    The base class of classes handling multimedia using GStreamer pipelines.
    """

    def __init__(self):
        """
        GStreamer multimedia streamer base class.
        """

        self.is_playing = False

        self._source_string = None
        self._pipeline_string = None
        self._sink_string = None
        self._launch_string = None

        self._pipeline = None

        self._msg_bus = None
        self._bus_handler_thread = None
        self._bus_handler_active = False
        self._registered_bus_callbacks = []

    def start(self):
        """Start the multimedia streaming"""

        # Start bus handler
        self._bus_handler_thread = threading.Thread(target=self._handle_bus_msg)
        self._bus_handler_thread.daemon = True
        self._bus_handler_active = True
        self._bus_handler_thread.start()
        self._notify('debug', 'Bus handler started.')

        # Start pipeline
        self._pipeline.set_state(STREAM_STATE_PLAYING)
        self._notify('info', 'Pipeline started.')
        self.is_playing = True

    def stop(self):
        """Stop multimedia streaming"""
        self._stop()
        self._bus_handler_thread.join()

    def _stop(self):
        """Stop without waiting for bus thread joining"""

        # Stop pipeline
        self._pipeline.set_state(STREAM_STATE_STOPPED)
        self._notify('info', 'Pipeline stopped.')
        self.is_playing = False

        # Stop bus handler
        self._bus_handler_active = False
        self._notify('debug', 'Bus handler stopped.')

    def build_pipeline(self):
        """Build the pipeline"""
        self._pre_build_pipeline()
        success = self._build_pipeline()
        if success:
            self._post_build_pipeline()

    def _build_pipeline(self):
        """
        Actually build the pipeline
        :return: Is building successful
        """
        raise NotImplementedError(
            "_build_pipeline should be implemented in subclass")

    def _check_if_can_update(self):
        """Pre-process before updating pipeline, sink or sources"""
        if self.is_playing:
            self._notify('err', "Can't update pipeline while playing.")
            raise RuntimeError("Can't update pipeline while playing.")

    def _pre_build_pipeline(self):
        """Pre-process before building pipeline"""

        if self.is_playing:
            self._notify('err', "Can't rebuild pipeline while playing.")
            raise RuntimeError("Can't rebuild pipeline while playing.")

        if self._pipeline is not None:
            self._notify('debug', "Setting pipeline state to NULL.")
            self._pipeline.set_state(Gst.State.NULL)

    def _post_build_pipeline(self):
        """Post-process after building pipeline"""
        self._msg_bus = self._pipeline.get_bus()
        self._register_bus_callback(Gst.MessageType.ERROR, self._on_error)

    def _handle_bus_msg(self):
        """Method handling bus messages"""
        while self._bus_handler_active:
            msg = self._msg_bus.timed_pop_filtered(10000, Gst.MessageType.ANY)
            if msg is not None:
                self._notify('debug', 'Bus message: %s' % (msg.type, ))
                for msg_type, callback in self._registered_bus_callbacks:
                    if msg.type == msg_type:
                        callback(msg)

    def _register_bus_callback(self, msg_type, callback):
        """Register callback to specific message type"""
        reg = BusCallback(msg_type, callback)
        self._registered_bus_callbacks.append(reg)

    def _on_error(self, msg):
        """Handles pipeline ERROR message"""
        err, debug = msg.parse_error()
        self._notify(
            'err', "Error in pipeline!\nError:\n{0}\n\nDebug Info:\n{1}".format(
                err, debug))
        self._stop()

    def _notify(self, severity, msg):
        """Notify event message."""
        print('{0}: {1}'.format(severity.upper(), msg))


class GstStreamer(GstBase):
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

        super(GstStreamer, self).__init__()

        self.update_sink(host, port, sink_override)
        self.update_pipeline(pipeline_string)

    def update_pipeline(self, pipeline_string=DEFAULT_STREAMER_PIPELINE):
        """
        Update the pipeline string used for the multimedia processing.
        :param pipeline_string: A GStreamer launch string handling the
        multimedia processing.
        :return: None
        """
        self._check_if_can_update()
        self._pipeline_string = pipeline_string
        self.build_pipeline()

    def update_sink(self, host=DEFAULT_HOST, port=DEFAULT_PORT,
                    sink_override=None):
        """
        Parse the UDPSINK, or use sink_override if inputted.
        May receive lists of host_ips and ports, splits using TEE and QUEUE.
        :param host: [string] The host/IP/Multicast group to send the packets to
        :param port: [string] The port to send the packets to
        :param sink_override: [string] Overrides the default UDPSINK
        :return: None
        """
        self._check_if_can_update()

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

        self._sink_string = sink_string
        self.build_pipeline()

    def _build_pipeline(self):
        """
        Actually build the pipeline
        :return: Is building successful
        """
        pipeline_string = self._pipeline_string
        sink_string = self._sink_string
        if pipeline_string is None or sink_string is None:
            return False

        launch_string = '{0} ! {1}'.format(pipeline_string, sink_string)
        self._pipeline = Gst.parse_launch(launch_string)
        self._launch_string = launch_string

        self._notify('info', "{0} '{1}'".format(
            'Constructed new pipeline using the following launch string',
            launch_string,
        ))

        return True

    def _post_build_pipeline(self):
        """Post-process after building pipeline"""
        super(GstStreamer, self)._post_build_pipeline()
        self._register_bus_callback(Gst.MessageType.EOS, self._on_eos)

    def _on_eos(self, msg):
        """Handles pipeline EOS message"""
        self._notify('info', 'Reached EOS')
        self.stop()


class GstViewer(GstBase):
    """
    A class handling the reception of multimedia data and viewing it.
    """

    def __init__(self, pipeline_string=DEFAULT_STREAMER_PIPELINE,
                 port=DEFAULT_PORT, source_override=None, sink_override=None):
        """
        GStreamer multimedia receiver and viewer class.
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :param port: [string] The port to send the packets to
        :param source_override: [string] Overrides the default UDPSRC
        :param sink_override: [string] Overrides the default AUTOVIDEOSINK
        """

        super(GstViewer, self).__init__()

        self.update_source(port, source_override)
        self.update_sink(sink_override)
        self.update_pipeline(pipeline_string)

    def update_pipeline(self, pipeline_string=DEFAULT_STREAMER_PIPELINE):
        """
        Creates a launch string using the inputted pipline string and
        the sink string (default is UDPSINK).
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :return: None
        """
        self._check_if_can_update()
        self._pipeline_string = pipeline_string
        self.build_pipeline()

    def update_source(self, port=DEFAULT_PORT, source_override=None):
        """
        Parse the UDPSRC, or use sink_override if inputted.
        :param port: [string] The port to send the packets to
        :param source_override: [string] Overrides the default UDPSRC
        :return: None
        """
        self._check_if_can_update()

        if source_override is not None:
            self._source_string = source_override
        else:
            self._source_string = _format_udpsource(port)

        self.build_pipeline()

    def update_sink(self, sink_override=None):
        """
        Parse the AUTOVIDEOSINK, or return sink_override if inputted.
        :param sink_override: [string] Overrides the default AUTOVIDEOSINK
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        if sink_override is not None:
            self._sink_string = sink_override
        else:
            self._sink_string = "videoconvert ! autovideosink"

        self.build_pipeline()

    def _build_pipeline(self):
        """
        Actually build the pipeline
        :return: Is building successful
        """

        pipeline_string = self._pipeline_string
        source_string = self._source_string
        sink_string = self._sink_string
        if (pipeline_string is None or source_string is None or
                sink_string is None):
            return False

        launch_string = ' ! '.join(
            [source_string, pipeline_string, sink_string])
        self._pipeline = Gst.parse_launch(launch_string)
        self._launch_string = launch_string

        self._notify('info', "{0} '{1}'".format(
            'Constructed new pipeline using the following launch string',
            launch_string,
        ))

        return True

    def _image_received_callback(self, img_sample):
        """
        The default callback on image reception, designed to be
        override to a usefull callback
        """
        raise NotImplementedError()


class GstDispatcher(GstBase):
    """
    A class handling the reception of multimedia data and sending it
    via callback.
    """

    def __init__(self, pipeline_string=DEFAULT_STREAMER_PIPELINE,
                 port=DEFAULT_PORT, source_override=None):
        """
        GStreamer multimedia receiver and dispatcher class.
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :param port: [string] The port to send the packets to
        :param source_override: [string] Overrides the default UDPSRC
        """

        super(GstDispatcher, self).__init__()

        self._appsink = None

        self._sink_string = 'videoconvert name="convert"'

        self.update_source(port, source_override)
        self.update_pipeline(pipeline_string)

    def update_pipeline(self, pipeline_string=DEFAULT_STREAMER_PIPELINE):
        """
        Creates a launch string using the inputted pipline string and
        the sink string (default is UDPSINK).
        :param pipeline_string: A GStreamer launch string who's output
         linked to the sink element.
        :return: None
        """
        self._check_if_can_update()
        self._pipeline_string = pipeline_string
        self.build_pipeline()

    def update_source(self, port=DEFAULT_PORT, source_override=None):
        """
        Parse the UDPSRC, or use sink_override if inputted.
        :param port: [string] The port to send the packets to
        :param source_override: [string] Overrides the default UDPSRC
        :return: None
        """
        self._check_if_can_update()

        if source_override is not None:
            self._source_string = source_override
        else:
            self._source_string = _format_udpsource(port)

        self.build_pipeline()

    def update_sink(self, sink_override=None):
        """
        Parse the AUTOVIDEOSINK, or return sink_override if inputted.
        :param sink_override: [string] Overrides the default AUTOVIDEOSINK
        :return: None
        """

        if self.is_playing:
            self._notify('err', "Can't change pipeline while playing.")
            return -1

        if sink_override is not None:
            self._sink_string = sink_override
        else:
            self._sink_string = "videoconvert ! autovideosink"

        self.build_pipeline()

    def register_callback(self, func_ptr):
        """
        Register a callback function called when an image received
        :param func_ptr: The callback function
        :return: None
        """
        self._image_received_callback = func_ptr

    def _build_pipeline(self):
        """
        Actually build the pipeline
        :return: Is building successful
        """

        pipeline_string = self._pipeline_string
        source_string = self._source_string
        sink_string = self._sink_string
        if (pipeline_string is None or source_string is None or
                sink_string is None):
            return False

        launch_string = ' ! '.join(
            [source_string, pipeline_string, sink_string])
        self._pipeline = Gst.parse_launch(launch_string)
        self._launch_string = launch_string

        self._build_sink()

        self._notify('info', "{0} '{1}'".format(
            'Constructed new pipeline using the following launch string',
            launch_string,
        ))

        return True

    def _build_sink(self):
        """Connect the sink to the callback"""

        conv = self._pipeline.get_child_by_name('convert')

        sink = Gst.ElementFactory.make("appsink", "sink")
        sink.set_property("emit-signals", True)
        caps = Gst.caps_from_string(
            "video/x-raw, format=(string){BGR, GRAY8}; " +
            "video/x-bayer,format=(string){rggb,bggr,grbg,gbrg}")
        sink.set_property("caps", caps)
        sink.connect("new-sample", self._sample_received, sink)
        self._appsink = sink

        self._pipeline.add(sink)
        if not Gst.Element.link(conv, sink):
            raise RuntimeError("Elements (convert, sink) could not be linked.")

        self._notify('info', 'Connected to sink')

    def _sample_received(self, sink, data):
        """Internal callback for sample received message"""
        sample = sink.emit("pull-sample")
        buf = sample.get_buffer()
        caps = sample.get_caps()
        img_buffer = buf.extract_dup(0, buf.get_size())
        img_format = caps.get_structure(0).get_value('format')
        height = caps.get_structure(0).get_value('height')
        width = caps.get_structure(0).get_value('width')
        sample_data = {'format': img_format, 'height': height, 'width': width,
                       'buffer': img_buffer}
        self._image_received_callback(sample_data)
        return Gst.FlowReturn.OK

    def _image_received_callback(self, img_sample):
        """
        The default callback on image reception, designed to be
        override to a usefull callback
        """
        self._notify('info', 'Dummy callback for image (%s-%ix%i)' % (
            img_sample['format'], img_sample['width'], img_sample['height']))


BusCallback = collections.namedtuple('BusCallback', 'msg_type callback')


def _format_udpsource(port):
    """Utility func for parsing udpsink element string"""
    port_str = 'port={0}'.format(port)
    sink_str = 'udpsrc {0} ! queue'.format(port_str)
    return sink_str


def _format_udpsink(host, port):
    """Utility func for parsing udpsink element string"""
    host_str = 'host={0}'.format(host)
    port_str = 'port={0}'.format(port)
    sink_str = 'queue ! udpsink {0} {1}'.format(host_str, port_str)
    return sink_str

