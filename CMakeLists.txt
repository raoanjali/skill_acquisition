cmake_minimum_required(VERSION 3.8)
project(skill_acq)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)

install(PROGRAMS
  scripts/build_package_catalog.py
  scripts/package_catalog.py
  scripts/run_ros_target.py
  scripts/select_ros_target.py
  scripts/skill_acq.py
  DESTINATION lib/${PROJECT_NAME}
)

install(FILES
  README.md
  DESTINATION share/${PROJECT_NAME}
)

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/global_package_catalog.json")
  install(FILES
    global_package_catalog.json
    DESTINATION share/${PROJECT_NAME}
  )
endif()

if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/package_catalog.db")
  install(FILES
    package_catalog.db
    DESTINATION share/${PROJECT_NAME}
  )
endif()

ament_package()
