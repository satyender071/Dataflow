package com.loblaw.dataflow;

import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.cloud.bigquery.*;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

@Slf4j
class CustomFn extends DoFn<PubsubMessage, String> {

    ValueProvider<String> projectId;
    ValueProvider<String> keyRing;
    ValueProvider<String> keyId;
    ValueProvider<String> location;
    ValueProvider<String> bucketName;
    public CustomFn(FormIngestionPipeline.MyOptions options) {
        this.keyId = options.getKeyId();
        this.projectId = options.getProjectId();
        this.location = options.getLocationId();
        this.keyRing = options.getKeyRing();
        this.bucketName = options.getBucketName();
    }
    @ProcessElement
    public void process(ProcessContext context) throws InterruptedException, Descriptors.DescriptorValidationException {
        DecryptResponse decrypt = null;
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            ByteString data = ByteString.copyFrom(Objects.requireNonNull(context.element()).getPayload());
            CryptoKeyName keyVersionName = CryptoKeyName.of(projectId.get(),
                    location.get(), keyRing.get(), keyId.get());
            decrypt = client.decrypt(keyVersionName, data);
            log.info("Decrypted Data: {}", decrypt.getPlaintext().toStringUtf8());
            JSONObject jsonObject = new JSONObject(decrypt.getPlaintext().toStringUtf8());
            JSONObject object = jsonObject.getJSONObject("formMetaData");

            JSONObject formData = new JSONObject(jsonObject.getJSONObject("formData").get("formData").toString());
            String pureJson = JsonUnflattener.unflatten(formData.toString());
            JSONObject pureJsonFormat = new JSONObject(pureJson);
            log.info("json data: {}", pureJsonFormat);
            pureJsonFormat.put("formMetaData",object);

            String formName = String.valueOf(object.get("formName"));
            String fileName = fileName(formName);
            log.info("fileName is : {}", fileName);

            Storage storage = StorageOptions.newBuilder().
                    setProjectId(projectId.get()).build().getService();
            BlobId blobId = BlobId.of(Objects.requireNonNull(bucketName.get()), formName + "/"
                    + folderName() + fileName + ".json");
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            byte[] content = data.toByteArray();
            storage.createFrom(blobInfo, new ByteArrayInputStream(content));

            log.info("storage data: {}", content);
            context.output(pureJsonFormat.toString());
//                }

        } catch (IOException e) {
            log.error("Can not process the data successfully");
            e.printStackTrace();
        }
    }
    public  String fileName(String formName) {
        String fileName;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int hours = calendar.get(Calendar.HOUR_OF_DAY);
        int minutes = calendar.get(Calendar.MINUTE);
        int seconds = calendar.get(Calendar.SECOND);
        fileName = formName + hours + "_" + minutes +
                "_" + seconds + "_" ;
        return fileName;
    }

    public  String folderName() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        int month = calendar.get(Calendar.MONTH) + 1;
        Integer year = calendar.get(Calendar.YEAR);
        String date = year + "-" + (month < 10 ? ("0" + month) : (month));
        return date + "/";
    }
}
