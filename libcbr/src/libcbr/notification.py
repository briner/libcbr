#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on Aug 23, 2011

@author: briner
'''
import os
import logging
import socket

# within pydinf
import stackfunction
import mix
my_logger=logging.getLogger('MyLogger')

#
MEL_SENDER='unix-noreply@unige.ch'

last_record=None


class NotifyError(object):
    def __init__(self, lemail_root, mel_sender):
        self.lemail_root=lemail_root
        self.mel_sender=mel_sender
        self.drecipient_msg={}
        self.notification_enabled=True
        my_logger.debug('initialize the "NotifyError" class')
        self.current_lcontext_msg=[]
    def add(self, msg_or_lmsg, lrecipient=[], l_or_context_msg=[]):
        #print '1   msg_or_lmsg', msg_or_lmsg, 'l_or_context_msg', l_or_context_msg
        my_logger.debug('def NotifyError.add(msg_or_lmsg(%s), lrecipient(%s))' % (msg_or_lmsg, lrecipient)) # TODO:debug2
        #
        if type(msg_or_lmsg) != list:
            msg_or_lmsg=[msg_or_lmsg]
        #print '2   msg_or_lmsg', msg_or_lmsg, 'l_or_context_msg', l_or_context_msg
        if type(l_or_context_msg)!=list:
            l_or_context_msg=[l_or_context_msg]
        #print '3   msg_or_lmsg', msg_or_lmsg, 'l_or_context_msg', l_or_context_msg
        #
        # group message by their context
        #print '3.1 self.l_or_context_msg', self.current_lcontext_msg, 'cmp', self.current_lcontext_msg == l_or_context_msg
        if self.current_lcontext_msg == l_or_context_msg:            
            if l_or_context_msg:
                msg_or_lmsg=[' - %s' % msg for msg in msg_or_lmsg]
        else:
            self.current_lcontext_msg = l_or_context_msg
            msg_or_lmsg=l_or_context_msg + msg_or_lmsg
        #print '4   msg_or_lmsg', msg_or_lmsg, 'l_or_context_msg', l_or_context_msg
        #print ''
        #print ''
        #
        # distribute messages to their recipient
        lrecipient=list( set(self.lemail_root).union(set(lrecipient)) )
        if not self.drecipient_msg:
            stackfunction.stack_function.add(self.send_interrupted)
        for recipient in lrecipient:
            recipient=recipient.lower()
            for msg in msg_or_lmsg:
                if self.drecipient_msg.get(recipient):
                    self.drecipient_msg[recipient].append(msg)
                else:
                    self.drecipient_msg[recipient]=[msg]
    def send(self, was_interrupted=False):
        if not self.notification_enabled:
            self.drecipient_msg={}
        hostname=socket.gethostname()
        for recipient, lmsg in self.drecipient_msg.iteritems():
            sender=self.mel_sender
            subject='[dolly] from host (%s)' % hostname
            if was_interrupted:
                body="dolly was interrupted\n\n"+os.linesep.join(lmsg)
            else:
                body=os.linesep.join(lmsg)
            mix.send_email(sender, recipient, subject, body)
        if self.drecipient_msg:
            my_logger.info( 'notifications sent by email to [%s]' % ', '.join(self.drecipient_msg.keys()) )
        else:
            my_logger.info( 'no notification to send by email')
    def send_interrupted(self):
        self.send(was_interrupted=True)
    def disable_email(self):
        my_logger.info( 'email notification disabled')
        self.notification_enabled=False

notify=NotifyError(lemail_root=[' unix-bot@unige.ch'], mel_sender='unix-noreply@unige.ch')
