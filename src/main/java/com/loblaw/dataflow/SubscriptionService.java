//package com.loblaw.dataflow.service;
//
//import com.google.cloud.kms.v1.CryptoKeyName;
//import com.google.cloud.kms.v1.DecryptResponse;
//import com.google.cloud.kms.v1.KeyManagementServiceClient;
//import com.google.cloud.pubsub.v1.AckReplyConsumer;
//import com.google.cloud.pubsub.v1.MessageReceiver;
//import com.google.cloud.pubsub.v1.Subscriber;
//import com.google.protobuf.ByteString;
//import com.google.pubsub.v1.ProjectSubscriptionName;
//import com.google.pubsub.v1.PubsubMessage;
//import com.loblaw.dataflow.configuration.EncryptionConfig;
//import com.loblaw.dataflow.configuration.PubSubConfig;
//import com.loblaw.dataflow.model.FormInputData;
//import com.loblaw.dataflow.model.FormMetaData;
//import lombok.extern.slf4j.Slf4j;
//import org.json.JSONObject;
//import org.springframework.stereotype.Service;
//
//import javax.inject.Inject;
//import java.io.IOException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.concurrent.atomic.AtomicReference;
//
//@Slf4j
//@Service
//public class SubscriptionService {
//
//    @Inject
//    PubSubConfig pubSubConfig;
//    @Inject
//    EncryptionConfig encryptionConfig;
//
//    public FormInputData messageSubscriber() {
//        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(pubSubConfig.getProjectId(),
//                pubSubConfig.getSubscriptionId());
//        AtomicReference<FormInputData> formInputData = new AtomicReference<>(new FormInputData());
//        FormMetaData formMetaData = new FormMetaData();
//        MessageReceiver messageReceiver = (PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) -> {
//            log.info("Message Details...");
//            log.info("ID :" + pubsubMessage.getMessageId());
//            log.info("DATA: " + pubsubMessage.getData().toStringUtf8());
//            formInputData.set(decryptedMessage(pubsubMessage));
//            ackReplyConsumer.ack();
//        };
//
//        Subscriber subscriber = null;
//
//        try {
//            subscriber = Subscriber.newBuilder(subscriptionName, messageReceiver).build();
//            subscriber.startAsync().awaitRunning();
//            subscriber.awaitTerminated(2, TimeUnit.SECONDS);
//        } catch (TimeoutException e) {
//            subscriber.stopAsync();
//        }
//
//        return formInputData.get();
//    }
//
//    public FormInputData decryptedMessage(PubsubMessage pubsubMessage) {
//
//        DecryptResponse decrypt = null;
//        FormInputData formInputData = new FormInputData();
//        FormMetaData formMetaData = new FormMetaData();
//
//        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
//
//            ByteString data = pubsubMessage.getData();
//            CryptoKeyName keyVersionName = CryptoKeyName.of(pubSubConfig.getProjectId(), encryptionConfig.getLocationId(),
//                    encryptionConfig.getKeyRing(), encryptionConfig.getKeyId());
//            decrypt = client.decrypt(keyVersionName, data);
//            log.info("Decrypted Data: {}", decrypt.getPlaintext().toStringUtf8());
//            JSONObject jsonObject = new JSONObject(decrypt.getPlaintext().toStringUtf8());
//            formInputData.setFormData(jsonObject.get("formData").toString());
//            JSONObject object = new JSONObject(jsonObject.get("formMetaData").toString());
//            formMetaData.setFormName(object.get("formName").toString());
//            formMetaData.setVersion(object.get("version").toString());
//            formMetaData.setTeam(object.get("team").toString());
//            formMetaData.setOrganization(object.get("organization").toString());
//            formInputData.setFormMetaData(formMetaData);
//            log.info("json data", jsonObject.get("formMetaData"));
//            log.info("form Input Data print : {}", formInputData);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return formInputData;
//    }
//}
