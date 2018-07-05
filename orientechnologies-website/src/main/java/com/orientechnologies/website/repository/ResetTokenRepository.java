package com.orientechnologies.website.repository;

import com.orientechnologies.website.model.schema.dto.ResetToken;

/**
 * Created by Enrico Risa on 20/10/14.
 */
public interface ResetTokenRepository extends BaseRepository<ResetToken> {

  ResetToken findByToken(String login);

}
