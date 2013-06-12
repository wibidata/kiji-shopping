/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wibidata.shopping.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.mindrot.jbcrypt.BCrypt;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiDataRequestException;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.util.ResourceUtils;

import com.wibidata.shopping.KijiContextListener;

public class SignInServlet extends HttpServlet {
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    request.setCharacterEncoding("UTF-8");
    request.getRequestDispatcher("/WEB-INF/view/SignIn.jsp").forward(request, response);
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {

    request.setCharacterEncoding("UTF-8");

    final Kiji kiji = (Kiji) getServletContext().getAttribute(KijiContextListener.KIJI_ATTRIBUTE);

    final String login = request.getParameter("login");
    final String password = request.getParameter("password");

    if (null == login || login.isEmpty()
        || null == password || password.isEmpty()) {
      request.setAttribute("login", login);
      request.setAttribute("password", password);
      request.setAttribute("error", "All fields are required.");
      request.getRequestDispatcher("/WEB-INF/view/SignIn.jsp").forward(request, response);
      return;
    }

    final KijiTable userTable = kiji.openTable("kiji_shopping_user");
    final EntityId entityId = userTable.getEntityId(login);
    final KijiTableReader reader = userTable.openTableReader();
    try {
      // Authenticate the user.
      KijiDataRequestBuilder drBuilder = KijiDataRequest.builder();
      drBuilder.newColumnsDef().add("info", "password").add("info", "name");
      KijiDataRequest dataRequest = drBuilder.build();

      KijiRowData row = reader.get(entityId, dataRequest);
      if (!row.containsColumn("info", "password")
          || !BCrypt.checkpw(password, row.getMostRecentValue("info", "password").toString())) {
        request.setAttribute("login", login);
        request.setAttribute("error", "Invalid login or password.");
        request.getRequestDispatcher("/WEB-INF/view/SignIn.jsp").forward(request, response);
        return;
      }
      request.setAttribute("name", row.getMostRecentValue("info", "name").toString());
    } catch (KijiDataRequestException e) {
      throw new IOException(e);
    } finally {
      IOUtils.closeQuietly(reader);
      ResourceUtils.releaseOrLog(userTable);
    }

    // Set the login cookie for the user.
    request.getSession().setAttribute("login", login);
    request.getRequestDispatcher("/WEB-INF/view/SignedIn.jsp").forward(request, response);
  }
}
