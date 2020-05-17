from f1_2019_telemetry.packets import PacketHeader, PacketID, HeaderFieldsToPacketType, unpack_udp_packet, PacketCarStatusData_V1, PacketCarTelemetryData_V1, PacketCarSetupData_V1, PacketLapData_V1, PacketMotionData_V1, PacketSessionData_V1, PacketEventData_V1, PacketParticipantsData_V1, TrackIDs


class Game:

    def __init__(self):
        self.drivers = []
        self.sessionID = None
        self.sessionIDBrut = None
        self.init = False

    def IsInitialized(self):
        if not self.init :
            self.init = len(self.drivers) > 0 and self.sessionID != None

        return self.init


    def processMotion(self, packet:PacketMotionData_V1, time):
        json = []
        if not self.IsInitialized():
            return json
        i = 0
        for driver in self.drivers:
            dic = {}
            dic["sessionId"] = self.sessionID
            dic["sessionTime"] = packet.header.sessionTime
            dic["packetId"] = packet.header.packetId
            dic["driver"] = driver
            json.append(
                {
                "measurement": "MotionData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": packet.carMotionData[i].fields
                }
            )
            i = i + 1

        dic = {}
        dic["sessionId"] = self.sessionID
        dic["sessionTime"] = packet.header.sessionTime

        dic["packetId"] = packet.header.packetId
        fields = {}
        fields["localVelocityX"] = packet.localVelocityX
        fields["localVelocityY"] = packet.localVelocityY
        fields["localVelocityZ"] = packet.localVelocityZ
        fields["angularVelocityX"] = packet.angularVelocityX
        fields["angularVelocityY"] = packet.angularVelocityY
        fields["angularVelocityZ"] = packet.angularVelocityZ
        fields["angularAccelerationX"] = packet.angularAccelerationX
        fields["angularAccelerationY"] = packet.angularAccelerationY
        fields["angularAccelerationZ"] = packet.angularAccelerationZ
        fields["frontWheelsAngle"] = packet.frontWheelsAngle
        fields["suspensionPosition_RL"] = packet.suspensionPosition[0]
        fields["suspensionPosition_RR"] = packet.suspensionPosition[1]
        fields["suspensionPosition_FL"] = packet.suspensionPosition[2]
        fields["suspensionPosition_FR"] = packet.suspensionPosition[3]

        fields["suspensionVelocity_RL"] = packet.suspensionVelocity[0]
        fields["suspensionVelocity_RR"] = packet.suspensionVelocity[1]
        fields["suspensionVelocity_FL"] = packet.suspensionVelocity[2]
        fields["suspensionVelocity_FR"] = packet.suspensionVelocity[3]

        fields["suspensionAcceleration_RL"] = packet.suspensionAcceleration[0]
        fields["suspensionAcceleration_RR"] = packet.suspensionAcceleration[1]
        fields["suspensionAcceleration_FL"] = packet.suspensionAcceleration[2]
        fields["suspensionAcceleration_FR"] = packet.suspensionAcceleration[3]
        fields["wheelSpeed_RL"] = packet.wheelSpeed[0]
        fields["wheelSpeed_RR"] = packet.wheelSpeed[1]
        fields["wheelSpeed_FL"] = packet.wheelSpeed[2]
        fields["wheelSpeed_FR"] = packet.wheelSpeed[3]

        fields["wheelSlip_RL"] = packet.wheelSlip[0]
        fields["wheelSlip_RR"] = packet.wheelSlip[1]
        fields["wheelSlip_FL"] = packet.wheelSlip[2]
        fields["wheelSlip_FR"] = packet.wheelSlip[3]
        json.append(
            {
                "measurement": "MyMotionData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": fields
            }
        )


        return json


    def processCarSetup(self, packet : PacketCarSetupData_V1, time):
        json = []
        if not self.IsInitialized():
            return json
        i = 0
        for driver in self.drivers:
            dic = {}
            dic["sessionId"] = self.sessionID
            dic["sessionTime"] = packet.header.sessionTime

            dic["packetId"] = packet.header.packetId
            dic["driver"] = driver
            json.append(
                {
                "measurement": "CarSetupData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": packet.carSetups[i].fields
            }
            )
            i = i + 1
        return json

    def processCarTelemetry(self, packet : PacketCarTelemetryData_V1, time):
        json = []
        if not self.IsInitialized():
            return json
        i = 0
        for driver in self.drivers:
            dic = {}
            dic["sessionId"] = self.sessionID
            dic["sessionTime"] = packet.header.sessionTime

            dic["packetId"] = packet.header.packetId
            dic["driver"] = driver

            fields = packet.carTelemetryData[i].fields
            fields["brakesTemperature_RL"] = packet.carTelemetryData[i].brakesTemperature[0]
            fields["brakesTemperature_RR"] = packet.carTelemetryData[i].brakesTemperature[1]
            fields["brakesTemperature_FL"] = packet.carTelemetryData[i].brakesTemperature[2]
            fields["brakesTemperature_FR"] = packet.carTelemetryData[i].brakesTemperature[3]


            fields["tyresSurfaceTemperature_RL"] = packet.carTelemetryData[i].tyresSurfaceTemperature[0]
            fields["tyresSurfaceTemperature_RR"] = packet.carTelemetryData[i].tyresSurfaceTemperature[1]
            fields["tyresSurfaceTemperature_FL"] = packet.carTelemetryData[i].tyresSurfaceTemperature[2]
            fields["tyresSurfaceTemperature_FR"] = packet.carTelemetryData[i].tyresSurfaceTemperature[3]


            fields["tyresInnerTemperature_RL"] = packet.carTelemetryData[i].tyresInnerTemperature[0]
            fields["tyresInnerTemperature_RR"] = packet.carTelemetryData[i].tyresInnerTemperature[1]
            fields["tyresInnerTemperature_FL"] = packet.carTelemetryData[i].tyresInnerTemperature[2]
            fields["tyresInnerTemperature_FR"] = packet.carTelemetryData[i].tyresInnerTemperature[3]


            fields["tyresPressure_RL"] = packet.carTelemetryData[i].tyresPressure[0]
            fields["tyresPressure_RR"] = packet.carTelemetryData[i].tyresPressure[1]
            fields["tyresPressure_FL"] = packet.carTelemetryData[i].tyresPressure[2]
            fields["tyresPressure_FR"] = packet.carTelemetryData[i].tyresPressure[3]


            fields["surfaceType_RL"] = packet.carTelemetryData[i].surfaceType[0]
            fields["surfaceType_RR"] = packet.carTelemetryData[i].surfaceType[1]
            fields["surfaceType_FL"] = packet.carTelemetryData[i].surfaceType[2]
            fields["surfaceType_FR"] = packet.carTelemetryData[i].surfaceType[3]

            del fields["surfaceType"]
            del fields["tyresPressure"]
            del fields["tyresInnerTemperature"]
            del fields["tyresSurfaceTemperature"]
            del fields["brakesTemperature"]

            json.append(
                {
                "measurement": "CarTelemetryData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": fields
            }
            )
            i = i + 1
        return json

    def processCarStatus(self, packet : PacketCarStatusData_V1, time):
        json = []
        if not self.IsInitialized():
            return json
        i = 0
        for driver in self.drivers:
            dic = {}
            dic["sessionId"] = self.sessionID
            dic["sessionTime"] = packet.header.sessionTime
            dic["packetId"] = packet.header.packetId
            dic["driver"] = driver
            fields = packet.carStatusData[i].fields

            # RL, RR, FL, FR
            fields["tyresWear_RL"] = fields["tyresWear"][0]
            fields["tyresWear_RR"] = fields["tyresWear"][1]
            fields["tyresWear_FL"] = fields["tyresWear"][2]
            fields["tyresWear_FR"] = fields["tyresWear"][3]

            fields["tyresDamage_RL"] = fields["tyresDamage"][0]
            fields["tyresDamage_RR"] = fields["tyresDamage"][1]
            fields["tyresDamage_FL"] = fields["tyresDamage"][2]
            fields["tyresDamage_FR"] = fields["tyresDamage"][3]
            del fields["tyresWear"]
            del fields["tyresDamage"]
            json.append(
                {
                "measurement": "CarStatusData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": fields
            }
            )
            i = i + 1
        return json


    def processLap(self, packet : PacketLapData_V1, time):
        json = []
        if not self.IsInitialized():
            return json
        i = 0
        for driver in self.drivers:
            dic = {}
            dic["sessionId"] = self.sessionID
            dic["sessionTime"] = packet.header.sessionTime
            dic["packetId"] = packet.header.packetId
            dic["driver"] = driver
            json.append(
                {
                "measurement": "LapData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": packet.lapData[i].fields
            }
            )
            i = i + 1
        return json

    def mySessionId(self, packet, time):
        if packet.header.sessionUID != self.sessionIDBrut:
            self.sessionIDBrut = packet.header.sessionUID
            self.sessionID = time.strftime('%Y%m%d_%H%M') + "_" + TrackIDs[packet.trackId] + "_" + str(packet.m_formula)
        return self.sessionID

    def processSession(self, packet : PacketSessionData_V1, time):
        json = []
        self.sessionID = self.mySessionId(packet, time)
        if not self.IsInitialized():
            return json
        dic = {}
        dic["sessionTime"] = packet.header.sessionTime
        dic["sessionId"] = self.sessionID
        dic["packetId"] = packet.header.packetId

        i = 0
        for mz in packet.marshalZones:
            dic["MarshalZoneId"] = "MarshalZone" + str(i)
            json.append(
                {
                    "measurement": "MarshalZones",
                    "tags": dic,
                    "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "fields": mz.fields
                }
            )
            i = i + 1


        dic = {}
        dic["sessionTime"] = packet.header.sessionTime
        dic["sessionId"] = self.sessionID
        dic["packetId"] = packet.header.packetId
        fields = packet.fields
        del fields["marshalZones"]
        del fields["header"]
        json.append(
            {
                "measurement": "SessionData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": fields
            }
        )

        return json

    def processEvent(self, packet: PacketEventData_V1, time):
        json = []
        if not self.IsInitialized():
            return json

        dic = {}
        dic["sessionId"] = self.sessionID
        dic["sessionTime"] = packet.header.sessionTime
        dic["packetId"] = packet.header.packetId
        fields = packet.fields
        fields["eventStringCode"] = fields["eventStringCode"].decode("utf-8")
        del fields["header"]
        json.append(
            {
                "measurement": "EventData",
                "tags": dic,
                "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "fields": fields
            }
        )

        return json

    def processParticipant(self, packet: PacketParticipantsData_V1, time):
        json = []
        if not self.IsInitialized():
            numActiveCars = int(packet.numActiveCars)
            self.drivers = []
            for i in range(numActiveCars):
                self.drivers.append(packet.participants[i].name.decode("utf-8") )
            return json

        dic = {}
        dic["sessionId"] = self.sessionID
        dic["sessionTime"] = packet.header.sessionTime
        dic["packetId"] = packet.header.packetId
        fields = packet.fields
        del fields["header"]

        numActiveCars = int(packet.numActiveCars)
        self.drivers = []
        for i in range(numActiveCars):
            driver = packet.participants[i].name.decode("utf-8")
            self.drivers.append(driver)
            dic["driver"] = driver
            fields = packet.participants[i].fields
            fields["name"] = driver
            json.append(
                {
                    "measurement": "ParticipantData",
                    "tags": dic,
                    "time": time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "fields": packet.participants[i].fields
                }
            )

        return json

