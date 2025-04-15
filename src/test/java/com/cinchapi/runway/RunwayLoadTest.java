/*
 * Copyright (c) 2013-2025 Cinchapi Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cinchapi.runway;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Runway's loading functionality
 *
 * @author Jeff Nelson
 */
public class RunwayLoadTest extends RunwayBaseClientServerTest {

    @Test
    public void testRecordIsOnlyLoadedOnceSanityCheck() {
        // Test that Runway will use the same object reference across links when
        // bulk loading records
        B b = new B();
        b.name = "b";

        A a1 = new A();
        a1.name = "a1";
        a1.b = b;

        A a2 = new A();
        a2.name = "a2";
        a2.b = b;

        runway.save(b, a1, a2);
        Set<A> as = runway.load(A.class);
        B expected = null;
        for (A a : as) {
            if(expected == null) {
                expected = a.b;
            }
            else {
                Assert.assertSame(expected, a.b);
            }
        }
    }
    
    @Test
    public void testComplexObjectGraphSharedReferences() {        
        // Create some shared entities that will be referenced multiple times
        Company company = new Company("Acme Corp");
        Department engineering = new Department("Engineering");
        Department marketing = new Department("Marketing");
        Department sales = new Department("Sales");
        Location hq = new Location("Headquarters", "123 Main St");
        
        // Create employees that belong to different departments but same company
        Employee ceo = new Employee("John CEO", company);
        ceo.location = hq;
        
        Employee cto = new Employee("Jane CTO", company);
        cto.department = engineering;
        cto.location = hq;
        cto.manager = ceo;
        
        Employee dev1 = new Employee("Alice Developer", company);
        dev1.department = engineering;
        dev1.location = hq;
        dev1.manager = cto;
        
        Employee dev2 = new Employee("Bob Developer", company);
        dev2.department = engineering;
        dev2.location = hq;
        dev2.manager = cto;
        
        Employee marketingHead = new Employee("Carol Marketing", company);
        marketingHead.department = marketing;
        marketingHead.location = hq;
        marketingHead.manager = ceo;
        
        Employee salesHead = new Employee("Dave Sales", company);
        salesHead.department = sales;
        salesHead.location = hq;
        salesHead.manager = ceo;
        
        // Create projects with multiple team members
        Project projectA = new Project("Project Alpha");
        projectA.teamMembers.add(cto);
        projectA.teamMembers.add(dev1);
        projectA.teamMembers.add(dev2);
        projectA.department = engineering;
        
        Project projectB = new Project("Project Beta");
        projectB.teamMembers.add(dev1);
        projectB.teamMembers.add(marketingHead);
        projectB.department = engineering;
        
        Project projectC = new Project("Project Gamma");
        projectC.teamMembers.add(marketingHead);
        projectC.teamMembers.add(salesHead);
        projectC.department = marketing;
        
        // Save all entities
        runway.save(company, engineering, marketing, sales, hq, 
                ceo, cto, dev1, dev2, marketingHead, salesHead,
                projectA, projectB, projectC);
        
        // Load all projects and verify references
        Set<Project> projects = runway.load(Project.class);
        Assert.assertEquals(3, projects.size());
        
        // Create maps to track object identities
        Map<Long, Employee> employeeMap = new HashMap<>();
        Map<Long, Department> departmentMap = new HashMap<>();
        Map<Long, Company> companyMap = new HashMap<>();
        Map<Long, Location> locationMap = new HashMap<>();
        
        // Verify that the same object reference is used for shared entities
        for (Project project : projects) {
            for (Employee employee : project.teamMembers) {
                long employeeId = employee.id();
                
                if (employeeMap.containsKey(employeeId)) {
                    // If we've seen this employee before, it should be the same object reference
                    Assert.assertSame("Employee objects should be the same reference", 
                            employeeMap.get(employeeId), employee);
                }
                else {
                    employeeMap.put(employeeId, employee);
                }
                
                // Check company references
                Company employeeCompany = employee.company;
                long companyId = employeeCompany.id();
                
                if (companyMap.containsKey(companyId)) {
                    Assert.assertSame("Company objects should be the same reference", 
                            companyMap.get(companyId), employeeCompany);
                }
                else {
                    companyMap.put(companyId, employeeCompany);
                }
                
                // Check department references if present
                if (employee.department != null) {
                    Department employeeDept = employee.department;
                    long deptId = employeeDept.id();
                    
                    if (departmentMap.containsKey(deptId)) {
                        Assert.assertSame("Department objects should be the same reference", 
                                departmentMap.get(deptId), employeeDept);
                    }
                    else {
                        departmentMap.put(deptId, employeeDept);
                    }
                }
                
                // Check location references
                if (employee.location != null) {
                    Location employeeLocation = employee.location;
                    long locationId = employeeLocation.id();
                    
                    if (locationMap.containsKey(locationId)) {
                        Assert.assertSame("Location objects should be the same reference", 
                                locationMap.get(locationId), employeeLocation);
                    }
                    else {
                        locationMap.put(locationId, employeeLocation);
                    }
                }
                
                // Check manager references
                if (employee.manager != null) {
                    Employee manager = employee.manager;
                    long managerId = manager.id();
                    
                    if (employeeMap.containsKey(managerId)) {
                        Assert.assertSame("Manager objects should be the same reference", 
                                employeeMap.get(managerId), manager);
                    }
                    else {
                        employeeMap.put(managerId, manager);
                    }
                }
            }
            
            // Check project department references
            Department projectDept = project.department;
            long deptId = projectDept.id();
            
            if (departmentMap.containsKey(deptId)) {
                Assert.assertSame("Department objects should be the same reference", 
                        departmentMap.get(deptId), projectDept);
            }
            else {
                departmentMap.put(deptId, projectDept);
            }
        }
        
        // Verify that we have the expected number of unique objects
        Assert.assertEquals("Should have 6 unique employees", 6, employeeMap.size());
        Assert.assertEquals("Should have 3 unique departments", 3, departmentMap.size());
        Assert.assertEquals("Should have 1 unique company", 1, companyMap.size());
        Assert.assertEquals("Should have 1 unique location", 1, locationMap.size());
    }
    
    @Test
    public void testDeepNestedObjectGraphWithCycles() {        
        // Create a team structure with circular references
        Team team = new Team("Dream Team");
        
        // Create members with circular references back to the team
        Member leader = new Member("Leader");
        leader.team = team;
        
        Member member1 = new Member("Member 1");
        member1.team = team;
        member1.mentor = leader;
        
        Member member2 = new Member("Member 2");
        member2.team = team;
        member2.mentor = leader;
        
        Member member3 = new Member("Member 3");
        member3.team = team;
        member3.mentor = member1;
        
        // Add members to the team (circular reference)
        team.leader = leader;
        team.members.add(leader);
        team.members.add(member1);
        team.members.add(member2);
        team.members.add(member3);
        
        // Create tasks assigned to multiple members
        Task task1 = new Task("Important Task");
        task1.assignees.add(leader);
        task1.assignees.add(member1);
        
        Task task2 = new Task("Another Task");
        task2.assignees.add(member1);
        task2.assignees.add(member2);
        
        Task task3 = new Task("Third Task");
        task3.assignees.add(leader);
        task3.assignees.add(member2);
        task3.assignees.add(member3);
        
        // Add tasks to the team to ensure they're loaded in a single operation
        team.tasks.add(task1);
        team.tasks.add(task2);
        team.tasks.add(task3);
        
        // Save all entities
        runway.save(team, leader, member1, member2, member3, task1, task2, task3);
        
        // Load the team and all related entities in a single load operation
        Team loadedTeam = runway.load(Team.class, team.id());
        
        // Create maps to track object identities
        Map<Long, Member> memberMap = new HashMap<>();
        Map<Long, Task> taskMap = new HashMap<>();
        
        // Add team members to the map
        for (Member member : loadedTeam.members) {
            memberMap.put(member.id(), member);
        }
        
        // Verify team leader is the same object reference as in members collection
        Member loadedLeader = loadedTeam.leader;
        Assert.assertSame("Team leader should be the same object reference as in members collection",
                memberMap.get(loadedLeader.id()), loadedLeader);
        
        // Verify tasks reference the same member objects
        for (Task task : loadedTeam.tasks) {
            taskMap.put(task.id(), task);
            
            for (Member assignee : task.assignees) {
                Assert.assertSame("Task should reference the same member object",
                        memberMap.get(assignee.id()), assignee);
                
                // Verify circular reference back to team
                Assert.assertSame("Member should reference the same team object",
                        loadedTeam, assignee.team);
            }
        }
        
        // Verify mentor relationships use the same object references
        for (Member member : loadedTeam.members) {
            if (member.mentor != null) {
                Assert.assertSame("Mentor should be the same object reference",
                        memberMap.get(member.mentor.id()), member.mentor);
            }
        }
    }

    class A extends Record {
        String name;
        B b;
    }

    class B extends Record {
        String name;
    }
    
    // Additional model classes for the complex tests
    
    class Company extends Record {
        String name;
        
        public Company(String name) {
            this.name = name;
        }
    }
    
    class Department extends Record {
        String name;
        
        public Department(String name) {
            this.name = name;
        }
    }
    
    class Location extends Record {
        String name;
        String address;
        
        public Location(String name, String address) {
            this.name = name;
            this.address = address;
        }
    }
    
    class Employee extends Record {
        String name;
        Company company;
        Department department;
        Location location;
        Employee manager;
        
        public Employee(String name, Company company) {
            this.name = name;
            this.company = company;
        }
    }
    
    class Project extends Record {
        String name;
        Set<Employee> teamMembers = new HashSet<>();
        Department department;
        
        public Project(String name) {
            this.name = name;
        }
    }
    
    class Team extends Record {
        String name;
        Member leader;
        Set<Member> members = new HashSet<>();
        Set<Task> tasks = new HashSet<>();
        
        public Team(String name) {
            this.name = name;
        }
    }
    
    class Member extends Record {
        String name;
        Team team;
        Member mentor;
        
        public Member(String name) {
            this.name = name;
        }
    }
    
    class Task extends Record {
        String name;
        Set<Member> assignees = new HashSet<>();
        
        public Task(String name) {
            this.name = name;
        }
    }
}
