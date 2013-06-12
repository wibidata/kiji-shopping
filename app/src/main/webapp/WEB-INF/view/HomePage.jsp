<!--
  (c) Copyright 2013 WibiData, Inc.
 
  See the NOTICE file distributed with this work for additional
  information regarding copyright ownership.
 
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ page contentType="text/html;charset=UTF-8"%>
<!doctype html>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html;charset=UTF-8">
    <title>KijiShopping</title>
    <link rel="stylesheet" type="text/css" href="/bootstrap/css/bootstrap.min.css"/>
    <link rel="stylesheet" type="text/css" href="/static/css/shopping.css"/>
  </head>
  <body>
    <div class="container">
      <div class="page-header">
        <div class="account-info">
          <c:if test="${fn:length(login) gt 0}">
            <b>${login}</b> | <a href="/signout">Sign out</a>
          </c:if>
        </div>
        <a href="/">KijiShopping</a>
        <span style="font-size: 16pt; color: #38739D; padding-top: 1em">powered by Kiji</span>
      </div>
      <c:if test="${fn:length(login) eq 0}">
        <div class="hero-unit food-background">
          <div class="well">
            <h2>Don't know what you're looking for?</h2>
            <p>
              Browse your favorite products, discover new styles, and share
              them with your friends. KijiShopping will recommend content
              for you in real time as you explore.
            </p>
            <div class="pull-right" style="width:50%">
              <div class="btn-group">
                <a class="btn btn-large btn-primary" href="/createuser">Create an account</a>
                &nbsp;
                <a class="btn btn-large btn-success" href="/signin">Sign in</a>
              </div>
            </div>
          </div>
        </div>
      </c:if>

      <c:if test="${fn:length(login) gt 0}">
        <div class="row">
          <div class="span8">
            <div class="page-header">
              <h2>Products <small>recommended for you</small></h2>
            </div>
            <c:choose>
              <c:when test="${fn:length(productRecommendations) gt 0}">
                <div class="row">
                  <c:forEach var="product" items="${productRecommendations}" varStatus="i">
                    <div class="span4">
                      <div class="well dish" onclick="location = '/product?id=${product.id}'"
                           title="View ${fn:toLowerCase(product.name)} details">
                        <h4>${product.name}</h4>
                        <div class="thumbnail" style="float:right">
                          <img src="${product.thumbnail}"/>
                        </div>
                        <p class="description">
                          ${product.description}
                        </p>
                      </div>
                    </div>
                  </c:forEach>
                </div>
              </c:when>
              <c:otherwise>
                <p>Rate a few more products below to start seeing personalization recommendations.</p>
              </c:otherwise>
            </c:choose>
          </div>
          <div class="span4">
            <c:if test="${fn:length(recentRatings) gt 0}">
              <div class="alert alert-info">
                <h3>Your recent ratings</h3>
                <ol>
                  <c:forEach var="rating" items="${recentRatings}">
                    <li>
                      ${rating.productName} &mdash;
                      <c:choose>
                        <c:when test="${rating.value eq 1}">
                          <span class="yum">Love it!</span>
                        </c:when>
                        <c:when test="${rating.value eq 0}">
                          <span class="meh">It's OK.</span>
                        </c:when>
                        <c:when test="${rating.value eq -1}">
                          <span class="blech">Not for me.</span>
                        </c:when>
                      </c:choose>
                    </li>
                  </c:forEach>
                </ol>
              </div>
            </c:if>
          </div>
        </div>
      </c:if>

      <div class="page-header">
        <h2>Browse <small>products by category</small></h2>
      </div>
      <c:forEach var="category" items="${categories}">
        <c:if test="${fn:length(category.products) ge 3}">
          <h3 class="cuisine-name">${category.name}</h3>
          <div class="row cuisine" style="clear:both">
            <c:forEach var="product" items="${category.products}">
              <div class="span4">
                <div class="well dish" onclick="location = '/product?id=${product.id}'"
                     title="View ${fn:toLowerCase(category.name)} details">
                  <h4>${product.name}</h4>
                  <div class="thumbnail" style="float:right"><img src="${product.thumbnail}"/></div>
                  <p class="description">
                    ${product.description}
                  </p>
                </div>
              </div>
            </c:forEach>
          </div>
        </c:if>
      </c:forEach>
    </div>
  </body>
</html>
