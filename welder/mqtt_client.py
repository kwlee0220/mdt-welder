from __future__ import annotations

import json
import logging
from typing import Callable, Optional, Any

import paho.mqtt.client as mqtt

# 로깅 설정
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("mqtt_client")

# MQTT 설정
MQTT_BROKER = "localhost"
MQTT_PORT = 1883

class MQTTClient:
    def __init__(self, 
                client_id: str = "waveform_inspector", 
                broker: str = MQTT_BROKER, 
                port: int = MQTT_PORT,
                username: Optional[str] = None,
                password: Optional[str] = None):
        """MQTT 클라이언트 초기화"""
        self.client = mqtt.Client(client_id=client_id)
        self.broker = broker
        self.port = port
        
        # 인증 정보가 제공된 경우 설정
        if username is not None and password is not None:
            self.client.username_pw_set(username, password)
        
        # 콜백 함수 설정
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        
        # 메시지 처리 콜백
        self.message_callback: Optional[Callable[[str, Any], None]] = None
        
    def connect(self) -> None:
        """브로커에 연결"""
        try:
            self.client.connect(self.broker, self.port, 60)
            self.client.loop_start()
            logger.info(f"Connecting to MQTT broker {self.broker}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            
    def disconnect(self) -> None:
        """브로커 연결 해제"""
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Disconnected from MQTT broker")
        
    def subscribe(self, topic:str, callback: Callable[[str, Any], None]) -> None:
        """웰더 상태 토픽 구독"""
        self.message_callback = callback
        self.client.subscribe(topic)
        self.client.on_message = callback
        logger.info(f"Subscribed to topic: {topic}")
        
    def _on_connect(self, client, userdata, flags, rc):
        """연결 콜백"""
        if rc == 0:
            logger.info("Connected to MQTT broker successfully")
        else:
            logger.error(f"Failed to connect to MQTT broker, return code: {rc}")
            
    def _on_disconnect(self, client, userdata, rc):
        """연결 해제 콜백"""
        if rc != 0:
            logger.warning(f"Unexpected disconnection from MQTT broker, return code: {rc}")
            
    def _on_message(self, client, userdata, msg):
        """메시지 수신 콜백"""
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode('utf-8'))
            logger.debug(f"Received message on topic {topic}: {payload}")
            
            if self.message_callback is not None and topic == MQTT_TOPIC:
                self.message_callback(topic, payload)
        except json.JSONDecodeError:
            logger.error(f"Failed to decode JSON payload from topic {topic}")
        except Exception as e:
            logger.error(f"Error processing message from topic {topic}: {e}")


# 사용 예시
def process_status_message(topic: str, payload: Any) -> None:
    """웰더 상태 메시지 처리"""
    logger.info(f"Processing welder status: {payload}")
    # 여기에 상태 처리 로직 추가


if __name__ == "__main__":
    # 클라이언트 생성 및 연결
    client = MQTTClient()
    client.connect()
    
    # 상태 토픽 구독
    client.subscribe_to_status(process_status_message)
    
    try:
        # 메인 스레드가 종료되지 않도록 대기
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down MQTT client...")
    finally:
        client.disconnect() 