package guru.springframework.reactivemongo.services;

import guru.springframework.reactivemongo.domain.Customer;
import guru.springframework.reactivemongo.mappers.CustomerMapper;
import guru.springframework.reactivemongo.mappers.CustomerMapperImpl;
import guru.springframework.reactivemongo.model.CustomerDTO;
import guru.springframework.reactivemongo.repositories.CustomerRepository;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
@SpringBootTest
public class CustomerServiceImplTest {

    @Container
    @ServiceConnection
    public static MongoDBContainer mongoDBContainer = new MongoDBContainer("mongo:latest");

    @Autowired
    CustomerService customerService;

    @Autowired
    CustomerMapper customerMapper;

    @Autowired
    CustomerRepository customerRepository;

    CustomerDTO customerDTO;

    @BeforeEach
    void setUp() {
        customerDTO = customerMapper.customerToCustomerDto(getTestCustomer());
    }

    @Test
    @DisplayName("Test Save Customer Using Subscriber")
    void saveCustomerUseSubscriber() {

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        AtomicReference<CustomerDTO> atomicDto = new AtomicReference<>();

        Mono<CustomerDTO> savedMono = customerService.saveCustomer(Mono.just(customerDTO));

        savedMono.subscribe(savedDto -> {
            System.out.println(savedDto.getId());
            atomicBoolean.set(true);
            atomicDto.set(savedDto);
        });

        await().untilTrue(atomicBoolean);

        CustomerDTO persistedDto = atomicDto.get();
        assertThat(persistedDto).isNotNull();
        assertThat(persistedDto.getId()).isNotNull();
    }

    @Test
    @DisplayName("Test Save Customer Using Block")
    void testSaveCustomerUseBlock() {
        CustomerDTO savedDto = customerService.saveCustomer(Mono.just(getTestCustomerDto())).block();
        assertThat(savedDto).isNotNull();
        assertThat(savedDto.getId()).isNotNull();
    }

    @Test
    @DisplayName("Test Update Customer Using Block")
    void testUpdateBlocking() {
        final String newName = "New Customer Name";  // use final so cannot mutate
        CustomerDTO savedCustomerDto = getSavedCustomerDto();
        savedCustomerDto.setCustomerName(newName);

        CustomerDTO updatedDto = customerService.saveCustomer(Mono.just(savedCustomerDto)).block();

        //verify exists in db
        Assertions.assertNotNull(updatedDto);
        CustomerDTO fetchedDto = customerService.getById(updatedDto.getId()).block();
        Assertions.assertNotNull(fetchedDto);
        assertThat(fetchedDto.getCustomerName()).isEqualTo(newName);
    }

    @Test
    @DisplayName("Test Update Using Reactive Streams")
    void testUpdateStreaming() {
        final String newName = "New Customer Name";  // use final so cannot mutate

        AtomicReference<CustomerDTO> atomicDto = new AtomicReference<>();

        customerService.saveCustomer(Mono.just(getTestCustomerDto()))
                .map(savedCustomerDto -> {
                    savedCustomerDto.setCustomerName(newName);
                    return savedCustomerDto;
                })
                .flatMap(customerService::saveCustomer) // save updated customer
                .flatMap(savedUpdatedDto -> customerService.getById(savedUpdatedDto.getId())) // get from db
                .subscribe(atomicDto::set);

        await().until(() -> atomicDto.get() != null);
        assertThat(atomicDto.get().getCustomerName()).isEqualTo(newName);
    }

    @Test
    void testDeleteCustomer() {
        CustomerDTO customerToDelete = getSavedCustomerDto();

        customerService.deleteCustomerById(customerToDelete.getId()).block();

        Mono<CustomerDTO> expectedEmptyCustomerMono = customerService.getById(customerToDelete.getId());

        CustomerDTO emptyCustomer = expectedEmptyCustomerMono.block();

        assertThat(emptyCustomer).isNull();

    }

    @Test
    void testFindFirstByCustomerName() {
        CustomerDTO customerDTO = getSavedCustomerDto();

        AtomicBoolean atomicBoolean = new AtomicBoolean(false);
        Mono<CustomerDTO> foundDTO = customerService.findFirstByCustomerName(customerDTO.getCustomerName());

        foundDTO.subscribe(dto -> {
            System.out.println(dto.toString());
            atomicBoolean.set(true);
        });

        await().untilTrue(atomicBoolean);
    }

    public CustomerDTO getSavedCustomerDto(){
        return customerService.saveCustomer(Mono.just(getTestCustomerDto())).block();
    }

    public static CustomerDTO getTestCustomerDto(){
        return new CustomerMapperImpl().customerToCustomerDto(getTestCustomer());
    }

    public static Customer getTestCustomer() {
        return Customer.builder()
                .customerName("Space Dust")
                .build();
    }

}
