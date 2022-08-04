


def setup_security()




    async def get_authorization(token: str = Depends(oauth2_scheme)):
        authorization = fake_decode_token(token)
        return authorization

    @api.post("/token")
    async def login(form_data: OAuth2PasswordRequestForm = Depends()):
        # Lookup user by form_data.user and form_data.password
        # if not user_dict:
        #    raise HTTPException(status_code=400, detail="Incorrect username or password")
        # user = UserInDB(**user_dict)
        # hashed_password = fake_hash_password(form_data.password)
        # if not hashed_password == user.hashed_password:
        #    raise HTTPException(status_code=400, detail="Incorrect username or password")
        return {"access_token": "SOME_FAKE_TOKEN", "token_type": "bearer"}

    @api.get('/test-security')
    async def test_security(authorization: Authorization = Depends(get_authorization)):
        auth = dump(authorization)
        return auth
