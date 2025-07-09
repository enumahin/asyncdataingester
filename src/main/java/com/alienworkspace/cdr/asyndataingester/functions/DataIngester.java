package com.alienworkspace.cdr.asyndataingester.functions;

import com.alienworkspace.cdr.asyndataingester.model.EncounterType;
import com.alienworkspace.cdr.model.dto.metadata.AsyncIngestionPayload;
import com.alienworkspace.cdr.model.dto.metadata.FacilityDto;
import com.alienworkspace.cdr.model.dto.patient.PatientDto;
import com.alienworkspace.cdr.model.dto.person.PersonDto;
import com.alienworkspace.cdr.model.dto.visit.EncounterDto;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import java.util.function.Consumer;
import java.util.function.Function;

@Configuration
public class DataIngester {

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private StreamBridge streamBridge;

    private static final Logger logger = LoggerFactory.getLogger(DataIngester.class);

    @Bean
    public Function<Message<byte[]>, Message<EncounterDto>> ingestion() {
        return message -> {
            try {
                AsyncIngestionPayload payload = objectMapper.readValue(message.getPayload(), AsyncIngestionPayload.class);
                logger.info("Processing message: {}", payload);

                PatientDto patient = PatientDto.builder()
                        .patientId(payload.getPatient().getPatientId())
                        .allergies(payload.getPatient().getAllergies())
                        .patientIdentifiers(payload.getPatient().getPatientIdentifiers())
                        .patientPrograms(payload.getPatient().getPatientPrograms())
                        .facility(FacilityDto.builder()
                                .id(payload.getFacilityId().longValue())
                                .build())
                        .build();
                patient.setUuid(payload.getPatient().getUuid());
                patient.setCreatedAt(payload.getPatient().getCreatedAt());
                patient.setLastModifiedAt(payload.getPatient().getLastModifiedAt());
                patient.setVoided(payload.getPatient().getVoided());
                patient.setVoidedAt(payload.getPatient().getVoidedAt());

                // Call the patient ingestion function
                patientIngestion().accept(patient);

                PersonDto personDto = payload.getPatient().getPerson();

                // Call the demographic ingestion function
                demographicIngestion().accept(personDto);

                payload.getPatient().getVisitDtos().parallelStream()
                        .flatMap(visit -> visit.getEncounterDtos().stream())
                        .forEach(this::routeEncounter);
                return null;
            } catch (Exception e) {
                logger.error("Error processing message", e);
                throw new RuntimeException("Failed to process message", e);
            }
        };
    }

    @Bean
    public Consumer<EncounterDto> pharmacyIngestion() {
        logger.info("Executing pharmacy ingestion");
        return payload -> {
            logger.info("Executing pharmacy ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("pharmacyIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    public Consumer<EncounterDto> clinicalIngestion() {
        return (payload) -> {
            logger.info("Executing clinical ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("clinicalIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    public Consumer<EncounterDto> labIngestion() {
        return (payload) -> {
            logger.info("Executing lab ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("labIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    public Consumer<EncounterDto> testingServicesIngestion() {
        return (payload) -> {
            logger.info("Executing hts ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("testingServicesIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    public Consumer<EncounterDto> tuberculosisIngestion() {
        return (payload) -> {
            logger.info("Executing Tuberculosis ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("tuberculosisIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Bean
    public Consumer<PatientDto> patientIngestion() {
        return (payload) -> {
            logger.info("Executing patient ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("patientIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            logger.info("Patient ingestion payload: {}", payloadString);
        };
    }

    @Bean
    public Consumer<PersonDto> demographicIngestion() {
        return (payload) -> {
            logger.info("Executing demographic ingestion");
            String payloadString = null;
            try {
                payloadString = objectMapper.writeValueAsString(payload);
                streamBridge.send("demographicIngestion-out-0", payloadString);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            logger.info("Demographic ingestion payload: {}", payloadString);
        };
    }

    private void routeEncounter(EncounterDto encounter) {
        int encounterTypeId = encounter.getEncounterType();
        EncounterType type = EncounterType.fromValue(encounterTypeId);

        if (type == null) {
            logger.error("Unknown encounter type: {}", encounterTypeId);
            return;
        }

        switch (type) {
            case CLINICAL -> clinicalIngestion().accept(encounter);
            case PHARMACY -> pharmacyIngestion().accept(encounter);
            case LAB -> labIngestion().accept(encounter);
            case TB -> tuberculosisIngestion().accept(encounter);
            case TS -> testingServicesIngestion().accept(encounter);
        }
    }

}
