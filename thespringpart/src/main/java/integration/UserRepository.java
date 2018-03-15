package integration;

import org.springframework.data.repository.CrudRepository;

import containers.User;

/**
 * Created by nydiarra on 06/05/17.
 */
public interface UserRepository extends CrudRepository<User, Long> {
    User findByUsername(String username);
}
