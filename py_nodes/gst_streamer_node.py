#!/usr/bin/env python

from __future__ import print_function

import sys
import time

import rospy
import std_msgs.msg

import gst_engines

DEFAULT_PIPELINE_STRING = " ! ".join([
    "videotestsrc",
    "jpegenc",
    "rtpjpegpay",
    ])
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = '2539'
DEFAULT_AUTO_RESTART = True


class GstStreamerNode(object):
    """
    A ROS node class handling the generation and streaming of multimedia
    data.
    """

    def __init__(self):
        """GStreamer multimedia streamer ROS node class"""

        rospy.init_node('gst_streamer')

        self._is_playing_publisher = None

        host = rospy.get_param(
            '/gst_streamer_node/host_ip', DEFAULT_HOST)
        port = rospy.get_param(
            '/gst_streamer_node/port', DEFAULT_PORT)
        sink_override = rospy.get_param(
            '/gst_streamer_node/sink_override', None)
        pipeline_string = rospy.get_param(
            '/gst_streamer_node/pipeline_string', DEFAULT_PIPELINE_STRING)
        auto_restart = rospy.get_param(
            '/gst_streamer_node/auto_restart', DEFAULT_AUTO_RESTART)

        self._init_publishers()

        gst_engines.GstStreamer._notify = self._ros_log

        self._engine = gst_engines.GstStreamer(
            pipeline_string, host, port, sink_override)

        self._is_auto_restart = auto_restart

        self.start_engine()
        self.spin_loop()

    def start_engine(self):
        """Start the multimedia streaming"""
        rospy.loginfo('Starting engine.')
        self._engine.start()

    def stop_engine(self):
        """Stop multimedia streaming"""
        rospy.loginfo('Stopping engine.')
        self._engine.stop()

    def spin_loop(self):
        """The main spin loop"""

        rospy.loginfo('Starting main loop')
        rate = rospy.Rate(10)
        while not rospy.is_shutdown():
            self._is_playing_publisher.publish(self._engine.is_playing)
            if not self._engine.is_playing and self._is_auto_restart:
                rospy.logwarn("Restarting engine.")
                self.start_engine()
            rate.sleep()

        if self._engine.is_playing:
            self.stop_engine()

    def _init_publishers(self):
        """Initialize the publishers"""
        self._is_playing_publisher = rospy.Publisher(
            'is_playing', std_msgs.msg.Bool, queue_size=1)

    def _ros_log(self, severity, msg):
        """Log event messages"""
        if severity == 'fatal':
            rospy.logfatal(msg)
        elif severity == 'err':
            rospy.logerr(msg)
        elif severity == 'warn':
            rospy.logwarn(msg)
        elif severity == 'info':
            rospy.loginfo(msg)
        elif severity == 'debug':
            rospy.logdebug(msg)
        else:
            raise NotImplementedError('Unsupported severity')

if __name__ == '__main__':
    gst_streamer_node = GstStreamerNode()