#!/usr/bin/env python

from __future__ import print_function

import numpy

import rospy
import std_msgs.msg
import sensor_msgs.msg
import cv_bridge

import gst_engines

DEFAULT_PIPELINE_STRING = gst_engines.DEFAULT_DISPATCHER_PIPELINE
DEFAULT_PORT = gst_engines.DEFAULT_PORT
DEFAULT_AUTO_RESTART = True


class GstDispatcherNode(object):
    """
    A ROS node class handling the reception of multimedia data and
    redistributing it as sensor_msgs/Image ROS messages.
    """

    def __init__(self):
        """GStreamer multimedia dispatcher ROS node class"""

        rospy.init_node('gst_dispatcher')

        self._is_playing_publisher = None
        self._image_publisher = None

        port = str(rospy.get_param(
            '~port', DEFAULT_PORT))
        source_override = rospy.get_param(
            '~source_override', None)
        pipeline_string = rospy.get_param(
            '~pipeline_string', DEFAULT_PIPELINE_STRING)
        auto_restart = rospy.get_param(
            '~auto_restart', DEFAULT_AUTO_RESTART)

        gst_engines.GstDispatcher._notify = self._ros_log

        self._engine = gst_engines.GstDispatcher(
            pipeline_string, port, source_override)
        self._engine.register_callback(self._on_new_image)
        self._cv_bridge = cv_bridge.CvBridge()

        self._is_auto_restart = auto_restart

        self._init_publishers()
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
                rospy.logwarn("Auto-Restarting engine.")
                self.start_engine()
            rate.sleep()

        if self._engine.is_playing:
            self.stop_engine()

    def _on_new_image(self, img_data):
        """Callback on image reception"""
        img = gst_to_opencv(img_data)
        msg = self._cv_bridge.cv2_to_imgmsg(img, encoding='bgr8')
        self._image_publisher.publish(msg)

    def _init_publishers(self):
        """Initialize the publishers"""
        self._is_playing_publisher = rospy.Publisher(
            'gst_dispatcher_is_playing', std_msgs.msg.Bool, queue_size=1)
        self._image_publisher = rospy.Publisher(
            'video', sensor_msgs.msg.Image, queue_size=1)

    def _ros_log(self, severity, msg):
        """Log event messages"""
        if severity == 'fatal': rospy.logfatal(msg)
        elif severity == 'err': rospy.logerr(msg)
        elif severity == 'warn': rospy.logwarn(msg)
        elif severity == 'info': rospy.loginfo(msg)
        elif severity == 'debug': rospy.logdebug(msg)
        else: raise NotImplementedError('Unsupported severity')


def gst_to_opencv(img_data):
    arr = numpy.ndarray((img_data['height'], img_data['width'], 3),
                        buffer=img_data['buffer'], dtype=numpy.uint8)
    return arr


if __name__ == '__main__':
    gst_dispatcher_node = GstDispatcherNode()
