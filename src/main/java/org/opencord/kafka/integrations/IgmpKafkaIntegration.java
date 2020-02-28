/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencord.kafka.integrations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.onosproject.net.device.DeviceService;
import org.opencord.kafka.EventBusService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opencord.igmpproxy.IgmpStatisticsEvent;
import org.opencord.igmpproxy.IgmpStatisticsEventListener;
import org.opencord.igmpproxy.IgmpStatisticsService;

/**
 * Listens for IGMP events and pushes them on a Kafka bus.
 */

@Component(immediate = true)
public class IgmpKafkaIntegration {

    public Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindIgmpStatService",
            unbind = "unbindIgmpStatService")
    protected volatile IgmpStatisticsService ignore;
    protected IgmpStatisticsService igmpStatisticsService;

    private final IgmpStatisticsEventListener igmpStatisticsEventListener =
            new InternalIgmpStatisticsListner();

    //TOPIC
    private static final String IGMP_STATISTICS_TOPIC = "onos.igmp.stats.kpis";

    // IGMP stats event params
    private static final String UNKNOWN_IGMP_TYPE_PACKETS_RX_COUNTER =
       "unknownIgmpTypePacketsRxCounter";
    private static final String REPORTS_RX_WITH_WRONG_MODE_COUNTER =
       "reportsRxWithWrongModeCounter";
    private static final String FAIL_JOIN_REQ_INSUFF_PERMISSION_ACCESS_COUNTER =
       "failJoinReqInsuffPermissionAccessCounter";
    private static final String FAIL_JOIN_REQ_UNKNOWN_MULTICAST_IP_COUNTER =
       "failJoinReqUnknownMulticastIpCounter";
    private static final String UNCONFIGURED_GROUP_COUNTER =
       "unconfiguredGroupCounter";
    private static final String VALID_IGMP_PACKET_COUNTER =
       "validIgmpPacketCounter";
    private static final String IGMP_CHANNEL_JOIN_COUNTER =
       "igmpChannelJoinCounter";
    private static final String CURRENT_GRP_NUM_COUNTER =
       "currentGrpNumCounter";

    protected void bindIgmpStatService(IgmpStatisticsService igmpStatisticsService) {
        log.info("bindIgmpStatService");
        if (this.igmpStatisticsService == null) {
            log.info("Binding IgmpStastService");
            this.igmpStatisticsService = igmpStatisticsService;
            log.info("Adding listener on IgmpStatService");
            igmpStatisticsService.addListener(igmpStatisticsEventListener);
        } else {
            log.warn("Trying to bind IgmpStatService but it is already bound");
        }
    }

    protected void unbindIgmpStatService(IgmpStatisticsService igmpStatisticsService) {
        log.info("unbindIgmpStatService");
        if (this.igmpStatisticsService == igmpStatisticsService) {
            log.info("Unbinding IgmpStatService");
            this.igmpStatisticsService = null;
            log.info("Removing listener on IgmpStatService");
            igmpStatisticsService.removeListener(igmpStatisticsEventListener);
        } else {
            log.warn("Trying to unbind IgmpStatService but it is already unbound");
        }
    }

    @Activate
    public void activate() {
        log.info("Started IgmpKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        log.info("Stopped IgmpKafkaIntegration");
    }

    private void handleStat(IgmpStatisticsEvent event) {
        eventBusService.send(IGMP_STATISTICS_TOPIC, serializeStat(event));
        log.info("IGMPStatisticsEvent sent successfully");
    }

    private JsonNode serializeStat(IgmpStatisticsEvent event) {
        log.info("Serializing IgmpStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode igmpStatEvent = mapper.createObjectNode();
        igmpStatEvent.put(UNKNOWN_IGMP_TYPE_PACKETS_RX_COUNTER, event.subject().getUnknownIgmpTypePacketsRxCounter());
        igmpStatEvent.put(REPORTS_RX_WITH_WRONG_MODE_COUNTER, event.subject().getReportsRxWithWrongModeCounter());
        igmpStatEvent.put(FAIL_JOIN_REQ_INSUFF_PERMISSION_ACCESS_COUNTER,
            event.subject().getFailJoinReqInsuffPermissionAccessCounter());
        igmpStatEvent.put(FAIL_JOIN_REQ_UNKNOWN_MULTICAST_IP_COUNTER,
            event.subject().getFailJoinReqUnknownMulticastIpCounter());
        igmpStatEvent.put(UNCONFIGURED_GROUP_COUNTER, event.subject().getUnconfiguredGroupCounter());
        igmpStatEvent.put(VALID_IGMP_PACKET_COUNTER, event.subject().getValidIgmpPacketCounter());
        igmpStatEvent.put(IGMP_CHANNEL_JOIN_COUNTER, event.subject().getIgmpChannelJoinCounter());
        igmpStatEvent.put(CURRENT_GRP_NUM_COUNTER, event.subject().getCurrentGrpNumCounter());
        return igmpStatEvent;
    }

    public class InternalIgmpStatisticsListner implements
                              IgmpStatisticsEventListener {

        @Override
        public void event(IgmpStatisticsEvent igmpStatisticsEvent) {
            handleStat(igmpStatisticsEvent);
        }
    }
}
