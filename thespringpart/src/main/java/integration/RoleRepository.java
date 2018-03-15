package integration;

import org.springframework.data.repository.CrudRepository;

import containers.Role;

/**
 * Created by nydiarra on 06/05/17.
 */
public interface RoleRepository extends CrudRepository<Role, Long> {
}
