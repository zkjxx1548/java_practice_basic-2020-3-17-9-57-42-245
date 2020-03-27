package com.thoughtworks;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class StudentRepository {

  public void save(List<Student> students) {
      students.forEach(student -> {
        try {
          this.save(student);
        } catch (SQLException e) {
          e.printStackTrace();
        }
      });
  }

  public void save(Student student) throws SQLException {
    // TODO:
    Connection conn = DbUtil.getConnection();
    String sql = "INSERT INTO student(id, name, sex, admission_year, birthday, classroom)"
            + "values(" + "?,?,?,?,?,?)";
    PreparedStatement ptmt = conn.prepareStatement(sql);
    addData(ptmt, student);

    ptmt.executeLargeUpdate();
  }

  public List<Student> query() throws SQLException {
    // TODO:
    Connection conn = DbUtil.getConnection();
    Statement stmt = conn.createStatement();
    ResultSet rs = stmt.executeQuery("SELECT id, name, sex, admission_year, birthday, classroom FROM student");
    return addToListFromResultSet(rs);
  }

  public List<Student> queryByClassId(String classId) throws SQLException {
    // TODO:
    Connection conn = DbUtil.getConnection();
    String sql = "SELECT id, name, sex, admission_year, birthday, classroom FROM student " +
            "WHERE classroom = ? ORDER BY id DESC";
    PreparedStatement ptmt = conn.prepareStatement(sql);
    ptmt.setString(1, classId);
    ResultSet rs = ptmt.executeQuery();
    return addToListFromResultSet(rs);
  }

  public void update(String id, Student student) throws SQLException {
    // TODO:
    Connection conn = DbUtil.getConnection();
    String sql = "UPDATE student SET id = ?,name = ?,sex = ?,admission_year = ?,birthday = ?,classroom = ? WHERE id = ?";
    PreparedStatement preparedStatement = conn.prepareStatement(sql);
    addData(preparedStatement, student);
    preparedStatement.setString(7, id);

    preparedStatement.executeUpdate();
  }

  public void delete(String id) throws SQLException {
    // TODO:
    Connection conn = DbUtil.getConnection();
    String sql = "DELETE FROM student WHERE id = ?";
    PreparedStatement preparedStatement = conn.prepareStatement(sql);

    preparedStatement.setString(1, id);
    preparedStatement.executeUpdate();
  }

  public List<Student> addToListFromResultSet(ResultSet rs) throws SQLException {
    List<Student> students = new ArrayList<>();
    Student s;
    while (rs.next()) {
      s = new Student();
      s.setId(rs.getString("id"));
      s.setName(rs.getString("name"));
      s.setGender(rs.getString("sex"));
      s.setAdmissionYear(rs.getInt("admission_year"));
      s.setBirthday(rs.getDate("birthday"));
      s.setClassId(rs.getString("classroom"));

      students.add(s);
    }
    return students;
  }

  public void addData(PreparedStatement preparedStatement, Student student) throws SQLException {
    preparedStatement.setString(1, student.getId());
    preparedStatement.setString(2, student.getName());
    preparedStatement.setString(3, student.getGender());
    preparedStatement.setInt(4, student.getAdmissionYear());
    preparedStatement.setDate(5, new Date(student.getBirthday().getTime()));
    preparedStatement.setString(6, student.getClassId());
  }
}
